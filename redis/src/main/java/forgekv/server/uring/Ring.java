package forgekv.server.uring;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * A minimal, dependency-free {@code io_uring} binding (Linux &ge; 5.6), on top
 * of raw syscalls via the FFM API.
 *
 * <p>Only what the AOF writer needs: set up a ring, push {@code WRITE}/
 * {@code FSYNC} submissions (optionally linked so the fsync only runs after
 * the write succeeded), submit with one {@code io_uring_enter}, and reap
 * completions. An {@code eventfd} registered with the ring tells a watcher
 * thread when a completion is posted, so the shard's event loop never blocks
 * on the disk.
 *
 * <p>Memory ordering follows the kernel ABI: we publish SQ entries with a
 * release store of the SQ tail, and consume CQ entries after an acquire load
 * of the CQ tail, releasing the CQ head when done.
 *
 * <p>Not thread-safe: a ring lives on its shard's thread.
 */
public final class Ring implements AutoCloseable {
    public static final byte OP_FSYNC = 3;
    public static final byte OP_WRITE = 23;
    /** {@code IOSQE_IO_LINK}: the next SQE only starts once this one succeeded. */
    public static final byte SQE_IO_LINK = 1 << 2;
    /** {@code IORING_FSYNC_DATASYNC}: fdatasync semantics. */
    public static final int FSYNC_DATASYNC = 1;

    private static final int ENTER_GETEVENTS = 1;
    private static final int REGISTER_EVENTFD = 4;
    private static final long OFF_SQ_RING = 0;
    private static final long OFF_CQ_RING = 0x0800_0000L;
    private static final long OFF_SQES = 0x1000_0000L;
    private static final int FEAT_SINGLE_MMAP = 1;

    private static final int PARAMS_SIZE = 120;
    private static final int SQE_SIZE = 64;
    private static final int CQE_SIZE = 16;

    private static final VarHandle INT = ValueLayout.JAVA_INT.varHandle();

    /** A completion queue entry ({@code struct io_uring_cqe}). */
    public record Cqe(long userData, int res, int flags) {}

    private final int fd;
    private final MemorySegment sqMap;
    private final MemorySegment cqMap; // == sqMap with IORING_FEAT_SINGLE_MMAP
    private final MemorySegment sqes;

    private final long sqHead;
    private final long sqTail;
    private final int sqMask;
    private final int sqEntries;
    private final long sqArray;
    private int toSubmit;

    private final long cqHead;
    private final long cqTail;
    private final int cqMask;
    private final long cqes;
    private boolean closed;

    private Ring(int fd, MemorySegment sqMap, MemorySegment cqMap, MemorySegment sqes, MemorySegment p) {
        this.fd = fd;
        this.sqMap = sqMap;
        this.cqMap = cqMap;
        this.sqes = sqes;
        // struct io_sqring_offsets at 40, struct io_cqring_offsets at 80.
        sqHead = u32(p, 40);
        sqTail = u32(p, 44);
        sqMask = sqMap.get(ValueLayout.JAVA_INT, u32(p, 48));
        sqEntries = sqMap.get(ValueLayout.JAVA_INT, u32(p, 52));
        sqArray = u32(p, 64);
        cqHead = u32(p, 80);
        cqTail = u32(p, 84);
        cqMask = cqMap.get(ValueLayout.JAVA_INT, u32(p, 88));
        cqes = u32(p, 100);
    }

    private static long u32(MemorySegment s, long off) {
        return Integer.toUnsignedLong(s.get(ValueLayout.JAVA_INT, off));
    }

    /** Create a ring with room for {@code entries} in-flight submissions. */
    public static Ring create(int entries) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment p = arena.allocate(PARAMS_SIZE, 8);
            int fd = (int) Native.syscall(Native.SYS_IO_URING_SETUP, entries, p.address());
            try {
                long sqEntries = u32(p, 0);
                long cqEntries = u32(p, 4);
                long features = u32(p, 20);
                long sqLen = u32(p, 64) + sqEntries * 4;
                long cqLen = u32(p, 100) + cqEntries * CQE_SIZE;
                boolean single = (features & FEAT_SINGLE_MMAP) != 0;
                MemorySegment sq = Native.mmap(fd, single ? Math.max(sqLen, cqLen) : sqLen, OFF_SQ_RING);
                MemorySegment cq = single ? sq : Native.mmap(fd, cqLen, OFF_CQ_RING);
                MemorySegment sqes = Native.mmap(fd, sqEntries * SQE_SIZE, OFF_SQES);
                return new Ring(fd, sq, cq, sqes, p);
            } catch (IOException | RuntimeException e) {
                Native.close(fd);
                throw e;
            }
        }
    }

    /** Whether this kernel/sandbox lets us create a ring at all. */
    public static boolean available() {
        try (Ring r = create(2)) {
            return true;
        } catch (IOException | RuntimeException | LinkageError e) {
            return false;
        }
    }

    private boolean pushSqe(byte opcode, byte flags, int fileFd, long off, long addr, int len, int opFlags,
            long userData) {
        int tail = (int) INT.get(sqMap, sqTail); // we are the only producer
        int head = (int) INT.getAcquire(sqMap, sqHead);
        if (tail - head >= sqEntries) {
            return false;
        }
        int idx = tail & sqMask;
        MemorySegment sqe = sqes.asSlice((long) idx * SQE_SIZE, SQE_SIZE);
        sqe.fill((byte) 0);
        sqe.set(ValueLayout.JAVA_BYTE, 0, opcode);
        sqe.set(ValueLayout.JAVA_BYTE, 1, flags);
        sqe.set(ValueLayout.JAVA_INT, 4, fileFd);
        sqe.set(ValueLayout.JAVA_LONG, 8, off);
        sqe.set(ValueLayout.JAVA_LONG, 16, addr);
        sqe.set(ValueLayout.JAVA_INT, 24, len);
        sqe.set(ValueLayout.JAVA_INT, 28, opFlags);
        sqe.set(ValueLayout.JAVA_LONG, 32, userData);
        sqMap.set(ValueLayout.JAVA_INT, sqArray + (long) idx * 4, idx);
        INT.setRelease(sqMap, sqTail, tail + 1);
        toSubmit++;
        return true;
    }

    /**
     * Queue a {@code WRITE} of {@code buf} (native memory that must stay valid
     * until its completion) at file offset {@code offset}. Returns
     * {@code false} if the submission queue is full.
     */
    public boolean pushWrite(int fileFd, MemorySegment buf, long offset, byte flags, long userData) {
        return pushSqe(OP_WRITE, flags, fileFd, offset, buf.address(), (int) buf.byteSize(), 0, userData);
    }

    /** Queue an {@code FSYNC} (fdatasync semantics). */
    public boolean pushFsync(int fileFd, long userData) {
        return pushSqe(OP_FSYNC, (byte) 0, fileFd, 0, 0, 0, FSYNC_DATASYNC, userData);
    }

    /** Submit queued SQEs, optionally blocking until {@code wait} completions exist. */
    public void submit(int wait) throws IOException {
        int flags = wait > 0 ? ENTER_GETEVENTS : 0;
        while (true) {
            try {
                long n = Native.syscall(Native.SYS_IO_URING_ENTER, fd, toSubmit, wait, flags, 0, 0);
                toSubmit -= (int) Math.min(n, toSubmit);
                return;
            } catch (Native.NativeException e) {
                if (e.errno != Native.EINTR) {
                    throw e;
                }
            }
        }
    }

    /** Pop one completion, or {@code null}. */
    public Cqe pop() {
        int head = (int) INT.get(cqMap, cqHead);
        int tail = (int) INT.getAcquire(cqMap, cqTail);
        if (head == tail) {
            return null;
        }
        long at = cqes + (long) (head & cqMask) * CQE_SIZE;
        Cqe c = new Cqe(
                cqMap.get(ValueLayout.JAVA_LONG, at),
                cqMap.get(ValueLayout.JAVA_INT, at + 8),
                cqMap.get(ValueLayout.JAVA_INT, at + 12));
        INT.setRelease(cqMap, cqHead, head + 1);
        return c;
    }

    /** Have the kernel signal {@code efd} whenever a completion is posted. */
    public void registerEventfd(int efd) throws IOException {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment arg = arena.allocate(4, 4);
            arg.set(ValueLayout.JAVA_INT, 0, efd);
            Native.syscall(Native.SYS_IO_URING_REGISTER, fd, REGISTER_EVENTFD, arg.address(), 1);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        // Unmap before closing the fd.
        Native.munmap(sqes);
        if (cqMap != sqMap) {
            Native.munmap(cqMap);
        }
        Native.munmap(sqMap);
        Native.close(fd);
    }

    // ---------------------------------------------------------------------
    // eventfd + raw file helpers used by the AOF backend
    // ---------------------------------------------------------------------

    /** A blocking eventfd. */
    public static int eventfd() throws IOException {
        return Native.eventfd();
    }

    /** Block until the eventfd counter is non-zero, then reset it. */
    public static long waitEventfd(int efd) throws IOException {
        return Native.readEventfd(efd);
    }

    public static void signalEventfd(int efd) throws IOException {
        Native.signalEventfd(efd, 1);
    }

    /** Open (creating if needed) a file for positional writes; returns the fd. */
    public static int openForWrite(String path) throws IOException {
        return Native.openForWrite(path);
    }

    public static void closeFd(int fd) {
        Native.close(fd);
    }
}
