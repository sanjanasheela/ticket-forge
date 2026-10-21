package forgelog.log;

import forgelog.util.Log;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * One segment of a partition log: {@code <base>.log} holds the records,
 * {@code <base>.index} is a <b>dense</b>, memory-mapped array of int32 file
 * positions, one per record. Offsets are contiguous, so finding a record is
 * {@code index[offset - base]}: O(1), no search.
 *
 * <p>Appends are encoded into an in-memory write buffer and reach the file in
 * large writes ({@link #drain()}); reads drain first, so they always see
 * everything appended.
 *
 * <p>Not thread-safe: a segment belongs to one broker event loop.
 */
final class Segment {
    private static final int WRITE_BUFFER = 1 << 20;

    final long base;
    final Path logPath;
    final Path indexPath;
    private final FileChannel ch;
    private final FileChannel indexCh;
    private final MappedByteBuffer index;
    private final int capacity;
    private int count;
    /** Logical size, including bytes still in the write buffer. */
    private long size;
    /** Bytes actually handed to the file. */
    private long written;
    private ByteBuffer pending = ByteBuffer.allocate(0);

    private Segment(long base, Path dir, int capacity) throws IOException {
        this.base = base;
        this.capacity = capacity;
        this.logPath = dir.resolve(name(base) + ".log");
        this.indexPath = dir.resolve(name(base) + ".index");
        this.ch = FileChannel.open(logPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        this.indexCh = FileChannel.open(indexPath, StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        this.index = indexCh.map(FileChannel.MapMode.READ_WRITE, 0, (long) capacity * 4);
    }

    static String name(long base) {
        return String.format("%020d", base);
    }

    static Segment create(Path dir, long base, int capacity) throws IOException {
        Segment s = new Segment(base, dir, capacity);
        s.ch.truncate(0);
        return s;
    }

    /**
     * Open an existing segment. A sealed segment ({@code expectedCount >= 0})
     * is trusted after a consistency check of its index; the active one
     * ({@code -1}) is scanned record by record, and a torn or corrupt tail is
     * truncated away.
     */
    static Segment open(Path dir, long base, int capacity, long expectedCount) throws IOException {
        Segment s = new Segment(base, dir, capacity);
        s.size = s.ch.size();
        s.written = s.size;
        if (expectedCount >= 0 && expectedCount <= capacity && s.indexLooksValid((int) expectedCount)) {
            s.count = (int) expectedCount;
            return s;
        }
        s.scan();
        if (expectedCount >= 0 && s.count != expectedCount) {
            throw new CorruptRecordException(
                    "sealed segment " + s.logPath + " holds " + s.count + " records, expected " + expectedCount);
        }
        return s;
    }

    private boolean indexLooksValid(int n) throws IOException {
        if (n == 0) {
            return size == 0;
        }
        if (index.getInt(0) != 0) {
            return false;
        }
        long last = Integer.toUnsignedLong(index.getInt((n - 1) * 4));
        if (last + Record.OVERHEAD > size) {
            return false;
        }
        ByteBuffer hdr = readAt(last, 16);
        return last + 4 + hdr.getInt(0) == size && hdr.getLong(8) == base + n - 1;
    }

    /** Rebuild the index from the records, truncating at the first bad one. */
    private void scan() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1 << 20);
        buf.limit(0);
        long bufStart = 0;
        long pos = 0;
        count = 0;
        boolean eof = false;
        while (true) {
            int rel = (int) (pos - bufStart);
            buf.position(rel);
            Record r;
            try {
                r = Record.decode(buf);
            } catch (CorruptRecordException e) {
                Log.warn(logPath + ": " + e.getMessage() + "; truncating");
                break;
            }
            if (r == null) {
                if (eof) {
                    break; // torn tail
                }
                // Refill: keep the partial record, read more.
                ByteBuffer rest = buf.slice(rel, buf.limit() - rel);
                int need = rest.remaining() >= 4 ? rest.getInt(0) + 4 : 4;
                if (need > buf.capacity() && need <= Record.MAX_SIZE + 4) {
                    ByteBuffer bigger = ByteBuffer.allocate(need);
                    bigger.put(rest);
                    buf = bigger;
                } else {
                    buf.clear();
                    buf.put(rest);
                }
                bufStart = pos;
                int n = 0;
                while (buf.hasRemaining()) {
                    int got = ch.read(buf, bufStart + buf.position());
                    if (got <= 0) {
                        eof = true;
                        break;
                    }
                    n += got;
                }
                buf.flip();
                if (n == 0 && eof && buf.remaining() == 0) {
                    break;
                }
                continue;
            }
            if (r.offset != base + count || count >= capacity) {
                Log.warn(logPath + ": unexpected offset " + r.offset + " at position " + pos + "; truncating");
                break;
            }
            index.putInt(count * 4, (int) pos);
            count++;
            pos = bufStart + buf.position();
        }
        if (pos < size) {
            Log.warn(logPath + ": discarding " + (size - pos) + " bytes after the last valid record");
            ch.truncate(pos);
        }
        size = pos;
        written = pos;
    }

    long nextOffset() {
        return base + count;
    }

    int count() {
        return count;
    }

    long sizeBytes() {
        return size;
    }

    boolean hasRoomFor(int bytes, long maxBytes) {
        return count < capacity && (count == 0 || size + bytes <= maxBytes);
    }

    /** File position of the record at relative index {@code i} ({@code i == count}: end of file). */
    long position(int i) {
        return i == count ? size : Integer.toUnsignedLong(index.getInt(i * 4));
    }

    void append(Record r) {
        int len = r.encodedSize();
        if (pending.remaining() < len) {
            drainQuietly();
            if (pending.capacity() < len || pending.capacity() == 0) {
                pending = ByteBuffer.allocate(Math.max(WRITE_BUFFER, len));
            }
        }
        r.encode(pending);
        index.putInt(count * 4, (int) size);
        size += len;
        count++;
    }

    private void drainQuietly() {
        try {
            drain();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** Hand buffered appends to the file (page cache). */
    void drain() throws IOException {
        if (pending.position() == 0) {
            return;
        }
        pending.flip();
        while (pending.hasRemaining()) {
            written += ch.write(pending, written);
        }
        pending.clear();
    }

    void flush() throws IOException {
        drain();
        ch.force(false);
    }

    /** fsync what has been drained (safe to call from another thread while the owner waits). */
    void force() throws IOException {
        ch.force(false);
    }

    /** Make a segment that will receive no more appends fully durable (log and index). */
    void seal() throws IOException {
        flush();
        index.force();
    }

    ByteBuffer readAt(long position, int len) throws IOException {
        drain();
        ByteBuffer b = ByteBuffer.allocate(len);
        while (b.hasRemaining()) {
            if (ch.read(b, position + b.position()) < 0) {
                throw new CorruptRecordException("unexpected end of " + logPath);
            }
        }
        return b.flip();
    }

    long termOf(int i) throws IOException {
        return readAt(position(i) + 16, 8).getLong(0);
    }

    /**
     * Largest relative index {@code j} in {@code (from, limit]} such that
     * records {@code [from, j)} fit in {@code maxBytes}; at least {@code from + 1}.
     */
    int endWithin(int from, int limit, int maxBytes) {
        long start = position(from);
        int lo = from + 1;
        int hi = limit;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (position(mid) - start <= maxBytes) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    /** Records with relative indexes {@code [from, to)} as raw bytes. */
    ByteBuffer readRange(int from, int to) throws IOException {
        long start = position(from);
        return readAt(start, (int) (position(to) - start));
    }

    Region region(int from, int to) throws IOException {
        drain();
        long start = position(from);
        return new Region(ch, start, (int) (position(to) - start), base + to);
    }

    void truncateTo(int newCount) throws IOException {
        drain();
        size = position(newCount);
        written = size;
        count = newCount;
        ch.truncate(size);
    }

    void close() throws IOException {
        drain();
        ch.close();
        indexCh.close();
    }

    void delete() throws IOException {
        ch.close();
        indexCh.close();
        Files.deleteIfExists(logPath);
        Files.deleteIfExists(indexPath);
    }
}
