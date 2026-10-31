package forgelog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * One partition: a single append-only file of framed records plus an in-memory array of each record's file
 * position, so a read by offset is one positional read. The array is rebuilt by scanning the file at start
 * (8 bytes per record in memory; fine at this scale, a sparse on-disk index is the known next step).
 *
 * <p>Frame: {@code int length | int crc32(body) | body}, body = {@code long timestamp, key, type, eventId, value}.
 * A torn last frame (crash mid-write) fails its length or CRC check at recovery and is cut off, so a
 * crash loses at most records that were never acknowledged.
 */
final class PartitionLog implements AutoCloseable {

    private static final int MAX_READ_BYTES = 4 * 1024 * 1024;

    private final FileChannel channel;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition more = lock.newCondition();
    private long[] positions = new long[1024];
    private int count;
    private long end;

    PartitionLog(Path dir) throws IOException {
        Files.createDirectories(dir);
        channel = FileChannel.open(dir.resolve("log"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        recover();
    }

    private void recover() throws IOException {
        long size = channel.size();
        long p = 0;
        ByteBuffer head = ByteBuffer.allocate(8);
        while (p + 8 <= size) {
            head.clear();
            channel.read(head, p);
            head.flip();
            int len = head.getInt();
            int crc = head.getInt();
            if (len < 4 + 8 || p + 4 + len > size) {
                break;
            }
            ByteBuffer body = ByteBuffer.allocate(len - 4);
            channel.read(body, p + 8);
            CRC32 c = new CRC32();
            c.update(body.array());
            if ((int) c.getValue() != crc) {
                break;
            }
            add(p);
            p += 4 + len;
        }
        if (p < size) {
            channel.truncate(p);
        }
        end = p;
    }

    private void add(long position) {
        if (count == positions.length) {
            positions = Arrays.copyOf(positions, count * 2);
        }
        positions[count++] = position;
    }

    long endOffset() {
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }

    /** Appends the batch as one write (and one fsync if asked) and returns the offset of its first record. */
    long append(List<Rec> batch, boolean sync) throws IOException {
        ByteBuffer[] frames = new ByteBuffer[batch.size()];
        int total = 0;
        for (int i = 0; i < frames.length; i++) {
            frames[i] = encode(batch.get(i));
            total += frames[i].remaining();
        }
        ByteBuffer all = ByteBuffer.allocate(total);
        for (ByteBuffer f : frames) {
            all.put(f);
        }
        all.flip();
        lock.lock();
        try {
            long first = count;
            long at = end;
            while (all.hasRemaining()) {
                at += channel.write(all, at);
            }
            if (sync) {
                channel.force(false);
            }
            long p = end;
            for (ByteBuffer f : frames) {
                add(p);
                p += f.limit();
            }
            end = p;
            more.signalAll();
            return first;
        } finally {
            lock.unlock();
        }
    }

    /** Reads up to {@code max} records from {@code from}, waiting up to {@code waitMs} if there are none yet. */
    List<Rec> read(long from, int max, long waitMs) throws IOException, InterruptedException {
        long startPos;
        long endPos;
        int n;
        lock.lock();
        try {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMs);
            while (from >= count) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    return List.of();
                }
                more.awaitNanos(left);
            }
            n = (int) Math.min(max, count - from);
            startPos = positions[(int) from];
            while (n > 1 && (from + n < count ? positions[(int) from + n] : end) - startPos > MAX_READ_BYTES) {
                n--;
            }
            endPos = from + n < count ? positions[(int) from + n] : end;
        } finally {
            lock.unlock();
        }
        ByteBuffer buf = ByteBuffer.allocate((int) (endPos - startPos));
        long at = startPos;
        while (buf.hasRemaining()) {
            int r = channel.read(buf, at);
            if (r < 0) {
                throw new IOException("log ended early");
            }
            at += r;
        }
        buf.flip();
        List<Rec> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(decode(buf, from + i));
        }
        return out;
    }

    private static ByteBuffer encode(Rec r) {
        byte[] type = r.type() == null ? new byte[0] : r.type().getBytes(StandardCharsets.UTF_8);
        byte[] id = r.eventId() == null ? new byte[0] : r.eventId().getBytes(StandardCharsets.UTF_8);
        byte[] key = r.key() == null ? new byte[0] : r.key();
        int bodyLen = 8 + 4 + key.length + 4 + type.length + 4 + id.length + 4 + r.value().length;
        ByteBuffer b = ByteBuffer.allocate(8 + bodyLen);
        b.putInt(4 + bodyLen);
        b.putInt(0);                       // crc placeholder
        b.putLong(r.timestamp());
        b.putInt(key.length).put(key);
        b.putInt(type.length).put(type);
        b.putInt(id.length).put(id);
        b.putInt(r.value().length).put(r.value());
        CRC32 c = new CRC32();
        c.update(b.array(), 8, bodyLen);
        b.putInt(4, (int) c.getValue());
        b.flip();
        return b;
    }

    private static Rec decode(ByteBuffer b, long offset) {
        b.getInt();                        // length
        b.getInt();                        // crc, already verified when the file was scanned
        long ts = b.getLong();
        byte[] key = take(b);
        byte[] type = take(b);
        byte[] id = take(b);
        byte[] value = take(b);
        return new Rec(offset, ts, key.length == 0 ? null : key, new String(type, StandardCharsets.UTF_8),
                new String(id, StandardCharsets.UTF_8), value);
    }

    private static byte[] take(ByteBuffer b) {
        byte[] x = new byte[b.getInt()];
        b.get(x);
        return x;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
