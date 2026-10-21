package forgekv.server.aof;

import forgekv.server.uring.Ring;
import forgekv.storage.Db;
import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import forgekv.util.Log;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * One shard's append handle.
 *
 * <p>The {@code appendfsync} policies follow Redis: {@code always} fsyncs
 * before replying, {@code everysec} fsyncs once a second (at most ~1s of
 * acknowledged writes can be lost), and {@code no} leaves it to the OS. Each
 * flush gets a sequence number, and replies wait until {@link #durableSeq()}
 * covers the sequence they depend on.
 *
 * <p>Two I/O backends implement this:
 * <ul>
 *   <li><b>std</b>: {@code write(2)} on the event loop. Under {@code always},
 *       the shard runs one {@code fdatasync} per event-loop pass, covering
 *       every connection served in that pass (the same group commit Redis
 *       does in {@code beforeSleep}), and the loop blocks while it runs. Under
 *       {@code everysec}, the fsync runs on a background thread.
 *   <li><b>io_uring</b>: a linked {@code WRITE → FSYNC} chain is submitted and
 *       the loop carries on serving clients while the disk works. Writes that
 *       arrive meanwhile are batched behind the in-flight chain (group
 *       commit), so there's at most one chain in flight. That keeps the file
 *       free of holes, since a crash can only ever tear the tail.
 * </ul>
 *
 * <p>Not thread-safe: owned by one shard thread.
 */
public final class Aof implements AutoCloseable {
    private static final long UD_WRITE = 1;
    private static final long UD_FSYNC = 2;

    /** Background {@code everysec} fsyncs, so the event loop never stalls on the disk. */
    private static final ExecutorService FSYNC_POOL = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "forgekv-fsync");
        t.setDaemon(true);
        return t;
    });

    private final Path path;
    private final FsyncPolicy fsync;
    /** Appended but not yet handed to the kernel. */
    private final ByteBuf buf = new ByteBuf(64 * 1024);
    /** Bytes the kernel has accepted into the file. */
    private long size;
    /** Sequence number of the latest flush. */
    private long seq;
    /** Every flush up to this sequence is persisted per the fsync policy. */
    private long durable;
    /** fsyncs issued (exposed as {@code aof_fsyncs}: writes ÷ fsyncs = group size). */
    private long fsyncs;

    // Exactly one backend is non-null.
    private Std std;
    private Uring uring;

    private static final class Std {
        FileChannel file;
        boolean dirty;
        /** Written under {@code always}, waiting for the group fsync. */
        boolean syncPending;
    }

    private static final class Uring {
        Ring ring;
        int fd = -1;
        /** Same file, for synchronous positional writes, fsync and size. */
        FileChannel file;
        int efd = -1;
        Inflight inflight;
        boolean fsyncWanted;
        /** Written since the last completed fsync. */
        boolean dirty;
        /** Staging buffer the kernel reads from (only one chain is ever in flight). */
        Arena arena;
        MemorySegment nbuf;
        Thread watcher;
        volatile boolean stopping;
    }

    private static final class Inflight {
        final long start;
        final long len;
        final long offset;
        /** Flush sequence that becomes durable when this chain completes. */
        final long upto;
        final boolean fsync;
        Integer writeRes;
        Integer fsyncRes;

        Inflight(long start, long len, long offset, long upto, boolean fsync) {
            this.start = start;
            this.len = len;
            this.offset = offset;
            this.upto = upto;
            this.fsync = fsync;
        }

        boolean done() {
            return writeRes != null && (!fsync || fsyncRes != null);
        }
    }

    private Aof(Path path, FsyncPolicy fsync) {
        this.path = path;
        this.fsync = fsync;
    }

    /** Open (creating if needed) one AOF file for appending. */
    public static Aof open(Path path, FsyncPolicy fsync, IoBackend backend) throws IOException {
        Aof a = new Aof(path, fsync);
        switch (backend) {
            case STD -> a.openStd();
            case URING -> a.openUring();
            case AUTO -> {
                try {
                    a.openUring();
                } catch (IOException | RuntimeException | LinkageError e) {
                    Log.warn("io_uring unavailable (" + e.getMessage() + "); using the std AOF backend");
                    a.openStd();
                }
            }
        }
        a.size = a.std != null ? a.std.file.size() : a.uring.file.size();
        return a;
    }

    private void openStd() throws IOException {
        Std s = new Std();
        s.file = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        std = s;
    }

    private void openUring() throws IOException {
        Uring u = new Uring();
        try {
            u.ring = Ring.create(8);
            u.efd = Ring.eventfd();
            u.ring.registerEventfd(u.efd);
            // Explicit offsets instead of O_APPEND: we always know where the tail is.
            u.fd = Ring.openForWrite(path.toString());
            u.file = FileChannel.open(path, StandardOpenOption.WRITE);
            u.arena = Arena.ofShared();
            u.nbuf = u.arena.allocate(64 * 1024, 4096);
        } catch (IOException | RuntimeException | LinkageError e) {
            closeUring(u);
            throw e;
        }
        uring = u;
    }

    public String backendName() {
        return std != null ? "std" : "io_uring";
    }

    public FsyncPolicy fsyncPolicy() {
        return fsync;
    }

    public boolean isUring() {
        return uring != null;
    }

    /** Bytes written to the file so far (excludes pending and in-flight data). */
    public long size() {
        return size;
    }

    public long fsyncs() {
        return fsyncs;
    }

    /** Sequence of the latest flush. */
    public long lastSeq() {
        return seq;
    }

    /** All flushes up to this sequence are persisted. */
    public long durableSeq() {
        return durable;
    }

    /**
     * io_uring only: start a daemon thread that blocks on the completion
     * eventfd and calls {@code onSignal} (from that thread) every time the
     * kernel posts completions. The callback must hand off to the owner
     * thread, which then calls {@link #onCompletion()}.
     */
    public void startCompletionWatcher(Runnable onSignal) {
        if (uring == null || uring.watcher != null) {
            return;
        }
        Uring u = uring;
        Thread t = new Thread(() -> {
            while (!u.stopping) {
                try {
                    Ring.waitEventfd(u.efd);
                } catch (IOException e) {
                    if (!u.stopping) {
                        Log.error("io_uring eventfd error: " + e.getMessage() + "; exiting");
                        Runtime.getRuntime().halt(1);
                    }
                    return;
                }
                if (u.stopping) {
                    return;
                }
                onSignal.run();
            }
        }, "forgekv-uring-" + path.getFileName());
        t.setDaemon(true);
        u.watcher = t;
        t.start();
    }

    /** Queue one command. Nothing touches the disk until {@link #flush()}. */
    public void append(Bytes... parts) {
        AofLog.encodeCommand(buf, parts);
    }

    /** Queue already-encoded commands. */
    public void appendRaw(Bytes bytes) {
        buf.put(bytes);
    }

    public void appendRaw(ByteBuf bytes) {
        buf.put(bytes);
    }

    /**
     * Hand pending commands to the kernel and return their sequence number.
     *
     * <p>std: written before returning. Under {@code always}, it becomes
     * durable at the next {@link #groupSync()}. io_uring: queued; the next
     * {@link #groupSync()} submits it as part of one chain shared by every
     * connection served meanwhile.
     */
    public long flush() throws IOException {
        if (buf.isEmpty()) {
            return seq;
        }
        seq++;
        if (std != null) {
            int len = buf.readable();
            writeFully(std.file, -1);
            size += len;
            std.dirty = true;
            if (fsync == FsyncPolicy.ALWAYS) {
                std.syncPending = true;
            } else {
                durable = seq;
            }
        }
        return seq;
    }

    /** Write all of {@code buf} (at {@code position}, or appending if -1) and clear it. */
    private void writeFully(FileChannel ch, long position) throws IOException {
        ByteBuffer bb = ByteBuffer.wrap(buf.array(), buf.readIndex(), buf.readable());
        long pos = position;
        while (bb.hasRemaining()) {
            int n = pos < 0 ? ch.write(bb) : ch.write(bb, pos);
            if (pos >= 0) {
                pos += n;
            }
        }
        buf.clear();
    }

    /** Work is waiting for {@link #groupSync()}. */
    public boolean needsGroupSync() {
        if (std != null) {
            return std.syncPending;
        }
        return uring.inflight == null && (!buf.isEmpty() || (uring.fsyncWanted && uring.dirty));
    }

    /**
     * Group commit point. std + {@code always}: one {@code fdatasync} for
     * everything written so far. io_uring: submit everything queued as one
     * chain, unless a chain is already in flight (then call again once it
     * completes).
     */
    public void groupSync() throws IOException {
        if (std != null) {
            if (std.syncPending) {
                std.file.force(false);
                fsyncs++;
                std.dirty = false;
                std.syncPending = false;
                durable = seq;
            }
        } else if (uring.inflight == null) {
            submitNext(fsync == FsyncPolicy.ALWAYS);
        }
    }

    /**
     * Process io_uring completions; call when the completion eventfd fires.
     * Afterwards, {@link #needsGroupSync()} says whether queued writes are
     * waiting for the next chain.
     */
    public void onCompletion() throws IOException {
        Uring u = uring;
        if (u == null) {
            return;
        }
        while (true) {
            Ring.Cqe c;
            while ((c = u.ring.pop()) != null) {
                Inflight f = u.inflight;
                if (f != null) {
                    if (c.userData() == UD_WRITE) {
                        f.writeRes = c.res();
                    } else if (c.userData() == UD_FSYNC) {
                        f.fsyncRes = c.res();
                    }
                }
            }
            Inflight f = u.inflight;
            if (f == null || !f.done()) {
                return;
            }
            u.inflight = null;
            int written = f.writeRes;
            if (written < 0) {
                throw new IOException("io_uring write failed: errno " + -written);
            }
            size += written;
            if (written < f.len) {
                // Short write: the linked fsync was cancelled. Resubmit the rest.
                pushChain(f.start + written, f.len - written, f.offset + written, f.upto, f.fsync);
                continue;
            }
            if (f.fsync) {
                if (f.fsyncRes != null && f.fsyncRes < 0) {
                    throw new IOException("io_uring fsync failed: errno " + -f.fsyncRes);
                }
                u.dirty = false;
                fsyncs++;
            } else if (f.len > 0) {
                u.dirty = true;
            }
            durable = f.upto;
            return;
        }
    }

    /** {@code appendfsync everysec} tick. */
    public void tickFsync() {
        if (std != null) {
            if (!std.dirty) {
                return;
            }
            std.dirty = false;
            FileChannel f = std.file;
            FSYNC_POOL.execute(() -> {
                try {
                    f.force(false);
                } catch (IOException e) {
                    Log.warn("background AOF fsync failed: " + e);
                }
            });
            return;
        }
        Uring u = uring;
        if (u.dirty || u.inflight != null) {
            u.fsyncWanted = true;
        }
        if (u.inflight == null && u.fsyncWanted) {
            try {
                submitNext(false);
            } catch (IOException e) {
                Log.warn("io_uring fsync submission failed: " + e.getMessage());
            }
        }
        // Otherwise the in-flight chain's completion triggers it.
    }

    /** Wait for in-flight I/O and write everything pending, synchronously. */
    private void drain() throws IOException {
        if (uring != null) {
            while (uring.inflight != null) {
                uring.ring.submit(1);
                onCompletion();
            }
            if (!buf.isEmpty()) {
                int len = buf.readable();
                writeFully(uring.file, size);
                size += len;
                seq++;
                uring.dirty = true;
            }
            durable = seq;
        } else {
            flush();
        }
    }

    /** Flush and fsync synchronously (graceful shutdown). */
    public void syncNow() throws IOException {
        drain();
        if (std != null) {
            std.file.force(false);
            std.dirty = false;
            std.syncPending = false;
            durable = seq;
        } else {
            uring.file.force(false);
            uring.dirty = false;
        }
    }

    /**
     * Compact the log: write the minimal command set that recreates
     * {@code db} to a temp file, fsync it, and atomically rename it over the AOF.
     *
     * <p>Runs synchronously on the shard's event loop (the dataset is
     * single-owner, so there is nothing to fork); expect a pause proportional
     * to the shard's dataset size.
     */
    public void rewrite(Db db, long now) throws IOException {
        drain();
        AofLog.writeSnapshot(path, db, now);
        if (std != null) {
            std.file.close();
            std.file = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
            std.dirty = false;
            std.syncPending = false;
            size = std.file.size();
        } else {
            uring.file.close();
            Ring.closeFd(uring.fd);
            uring.fd = -1;
            uring.fd = Ring.openForWrite(path.toString());
            uring.file = FileChannel.open(path, StandardOpenOption.WRITE);
            uring.dirty = false;
            size = uring.file.size();
        }
        durable = seq;
    }

    /** Submit everything in {@code buf} as one chain (plus an fsync if required). */
    private void submitNext(boolean always) throws IOException {
        Uring u = uring;
        boolean doFsync = always || u.fsyncWanted;
        if (buf.isEmpty() && !(doFsync && u.dirty)) {
            u.fsyncWanted = false;
            return;
        }
        u.fsyncWanted = false;
        int len = buf.readable();
        if (u.nbuf.byteSize() < len) {
            long cap = Math.max(len, u.nbuf.byteSize() * 2);
            u.arena.close();
            u.arena = Arena.ofShared();
            u.nbuf = u.arena.allocate(cap, 4096);
        }
        MemorySegment.copy(buf.array(), buf.readIndex(), u.nbuf, ValueLayout.JAVA_BYTE, 0, len);
        buf.clear();
        pushChain(0, len, size, seq, doFsync);
    }

    private void pushChain(long start, long len, long offset, long upto, boolean doFsync) throws IOException {
        Uring u = uring;
        boolean hasData = len > 0;
        if (hasData) {
            byte link = doFsync ? Ring.SQE_IO_LINK : 0;
            boolean ok = u.ring.pushWrite(u.fd, u.nbuf.asSlice(start, len), offset, link, UD_WRITE);
            assert ok : "ring sized for one chain";
        }
        if (doFsync) {
            boolean ok = u.ring.pushFsync(u.fd, UD_FSYNC);
            assert ok : "ring sized for one chain";
        }
        u.ring.submit(0);
        Inflight f = new Inflight(start, len, offset, upto, doFsync);
        if (!hasData) {
            f.writeRes = 0;
        }
        u.inflight = f;
    }

    /** Release the file (and the ring). In-flight io_uring I/O is waited for first. */
    @Override
    public void close() {
        if (std != null) {
            try {
                std.file.close();
            } catch (IOException e) {
                Log.warn("closing AOF: " + e);
            }
            return;
        }
        Uring u = uring;
        // The kernel may still be reading the in-flight buffer: wait for it.
        while (u.inflight != null) {
            try {
                u.ring.submit(1);
            } catch (IOException e) {
                break;
            }
            Ring.Cqe c;
            while ((c = u.ring.pop()) != null) {
                if (c.userData() == UD_WRITE) {
                    u.inflight.writeRes = c.res();
                } else {
                    u.inflight.fsyncRes = c.res();
                }
            }
            if (u.inflight.done()) {
                u.inflight = null;
            }
        }
        closeUring(u);
    }

    private static void closeUring(Uring u) {
        u.stopping = true;
        if (u.watcher != null && u.efd >= 0) {
            try {
                Ring.signalEventfd(u.efd);
                u.watcher.join(2000);
            } catch (IOException | InterruptedException e) {
                // best effort
            }
        }
        if (u.ring != null) {
            u.ring.close();
        }
        if (u.fd >= 0) {
            Ring.closeFd(u.fd);
        }
        if (u.efd >= 0) {
            Ring.closeFd(u.efd);
        }
        if (u.file != null) {
            try {
                u.file.close();
            } catch (IOException e) {
                // ignore
            }
        }
        if (u.arena != null) {
            u.arena.close();
        }
    }
}
