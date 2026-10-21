package forgelog.log;

import forgelog.util.Log;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * A partition's durable, segmented commit log, which is also the partition's
 * Raft log: a record's offset is its Raft index.
 *
 * <ul>
 *   <li>Segments roll at {@link Config#segmentBytes} or when the dense index is
 *       full; a rolled segment is fsynced (log and index) before the next one
 *       is used, so recovery only has to scan the active segment.
 *   <li>Recovery validates every record of the active segment (CRC32C, offset
 *       continuity) and truncates a torn tail left by a crash mid-write.
 *   <li>Term boundaries ({@link TermIndex}) are rebuilt without a full scan:
 *       terms never decrease, so each segment is bisected between its first
 *       and last record's terms.
 * </ul>
 *
 * <p>Not thread-safe: a log belongs to one broker event loop.
 */
public final class CommitLog implements RaftLog, Closeable {
    /**
     * @param segmentBytes roll the active segment past this size (at most 1 GiB)
     * @param indexEntries records per segment (the dense index holds 4 bytes each)
     */
    public record Config(long segmentBytes, int indexEntries) {
        public static final Config DEFAULT = new Config(256L << 20, 1 << 20);

        public Config {
            if (segmentBytes <= 0 || segmentBytes > (1L << 30) || indexEntries <= 0) {
                throw new IllegalArgumentException("bad log config");
            }
        }
    }

    /** Observes appends and truncations (the broker keeps producer state with it). */
    public interface Listener {
        void onAppend(Record r);

        void onTruncate(long fromOffset);
    }

    private final Path dir;
    private final Config cfg;
    private final List<Segment> segments = new ArrayList<>();
    private final TermIndex terms = new TermIndex();
    private boolean dirty;
    private Listener listener;

    private CommitLog(Path dir, Config cfg) {
        this.dir = dir;
        this.cfg = cfg;
    }

    /** Open (creating if needed) the log in {@code dir}, recovering from a crash if necessary. */
    public static CommitLog open(Path dir, Config cfg) throws IOException {
        Files.createDirectories(dir);
        CommitLog log = new CommitLog(dir, cfg);
        List<Long> bases = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            files.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".log") && n.length() == 24)
                    .forEach(n -> bases.add(Long.parseLong(n.substring(0, 20))));
        }
        bases.sort(null);
        for (int i = 0; i < bases.size(); i++) {
            long base = bases.get(i);
            boolean last = i == bases.size() - 1;
            long expected = last ? -1 : bases.get(i + 1) - base;
            Segment s = Segment.open(dir, base, cfg.indexEntries(), expected);
            if (!log.segments.isEmpty() && log.segments.getLast().nextOffset() != base) {
                throw new CorruptRecordException("gap in " + dir + " before segment " + base);
            }
            log.segments.add(s);
        }
        if (log.segments.isEmpty()) {
            log.segments.add(Segment.create(dir, 0, cfg.indexEntries()));
        } else if (log.segments.getFirst().base != 0) {
            throw new CorruptRecordException(dir + " does not start at offset 0");
        }
        for (Segment s : log.segments) {
            log.rebuildTerms(s);
        }
        return log;
    }

    private void rebuildTerms(Segment s) throws IOException {
        int n = s.count();
        if (n == 0) {
            return;
        }
        long lo = s.termOf(0);
        terms.onAppend(s.base, lo);
        bisect(s, 0, lo, n - 1, s.termOf(n - 1));
    }

    /** Record every term change between relative indexes {@code a} and {@code b}. */
    private void bisect(Segment s, int a, long termA, int b, long termB) throws IOException {
        if (termA == termB) {
            return;
        }
        if (b == a + 1) {
            terms.onAppend(s.base + b, termB);
            return;
        }
        int mid = (a + b) >>> 1;
        long termMid = s.termOf(mid);
        bisect(s, a, termA, mid, termMid);
        bisect(s, mid, termMid, b, termB);
    }

    public void setListener(Listener l) {
        this.listener = l;
    }

    public Path dir() {
        return dir;
    }

    public long nextOffset() {
        return segments.getLast().nextOffset();
    }

    @Override
    public long lastIndex() {
        return nextOffset() - 1;
    }

    @Override
    public long termAt(long index) {
        if (index == -1) {
            return 0;
        }
        if (index < 0 || index > lastIndex()) {
            return -1;
        }
        return terms.termAt(index);
    }

    @Override
    public long firstIndexOfTerm(long index) {
        return terms.startOf(index);
    }

    public int segmentCount() {
        return segments.size();
    }

    public long sizeBytes() {
        long total = 0;
        for (Segment s : segments) {
            total += s.sizeBytes();
        }
        return total;
    }

    @Override
    public void append(Record r) {
        if (r.offset != nextOffset()) {
            throw new IllegalArgumentException("append at " + r.offset + ", expected " + nextOffset());
        }
        int len = r.encodedSize();
        if (len > Record.MAX_SIZE) {
            throw new IllegalArgumentException("record too large: " + len + " bytes");
        }
        try {
            Segment active = segments.getLast();
            if (!active.hasRoomFor(len, cfg.segmentBytes())) {
                active.seal();
                active = Segment.create(dir, r.offset, cfg.indexEntries());
                segments.add(active);
                Log.debug(dir + ": rolled to segment " + r.offset);
            }
            active.append(r);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (UncheckedIOException e) {
            throw e;
        }
        terms.onAppend(r.offset, r.term);
        dirty = true;
        if (listener != null) {
            listener.onAppend(r);
        }
    }

    @Override
    public void truncateFrom(long index) {
        if (index > lastIndex()) {
            return;
        }
        long from = Math.max(0, index);
        try {
            while (segments.size() > 1 && segments.getLast().base >= from) {
                segments.removeLast().delete();
            }
            Segment last = segments.getLast();
            last.truncateTo((int) (from - last.base));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        terms.truncateFrom(from);
        dirty = true;
        if (listener != null) {
            listener.onTruncate(from);
        }
    }

    private Segment segmentFor(long offset) {
        int lo = 0;
        int hi = segments.size() - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (segments.get(mid).base <= offset) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return segments.get(lo);
    }

    @Override
    public List<Record> entries(long from, long toInclusive, int maxBytes) {
        List<Record> out = new ArrayList<>();
        long to = Math.min(toInclusive, lastIndex());
        if (from < 0 || from > to) {
            return out;
        }
        try {
            Segment s = segmentFor(from);
            int a = (int) (from - s.base);
            int limit = (int) (Math.min(to + 1, s.nextOffset()) - s.base);
            int b = s.endWithin(a, limit, maxBytes);
            ByteBuffer raw = s.readRange(a, b);
            while (raw.hasRemaining()) {
                out.add(Record.decode(raw));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out;
    }

    public Record read(long offset) {
        List<Record> r = entries(offset, offset, 1);
        return r.isEmpty() ? null : r.getFirst();
    }

    /**
     * Raw records {@code [from, endExclusive)} from a single segment, at most
     * about {@code maxBytes} (but at least one record), for zero-copy sends.
     * Returns {@code null} if there is nothing to send.
     */
    public Region region(long from, long endExclusive, int maxBytes) throws IOException {
        long end = Math.min(endExclusive, nextOffset());
        if (from < 0 || from >= end) {
            return null;
        }
        Segment s = segmentFor(from);
        int a = (int) (from - s.base);
        int limit = (int) (Math.min(end, s.nextOffset()) - s.base);
        int b = s.endWithin(a, limit, maxBytes);
        return s.region(a, b);
    }

    public boolean isDirty() {
        return dirty;
    }

    /**
     * Split flush, so a broker can fsync many logs in parallel: {@link #write}
     * on the owning thread, then {@link #sync} (any thread, while the owner
     * waits), then {@link #markFlushed} on the owning thread.
     */
    public void write() throws IOException {
        segments.getLast().drain();
    }

    public void sync() throws IOException {
        segments.getLast().force();
    }

    public void markFlushed() {
        dirty = false;
    }

    /** Write buffered appends and, if {@code fsync}, make them durable. */
    public void flush(boolean fsync) throws IOException {
        if (!dirty) {
            return;
        }
        Segment active = segments.getLast();
        if (fsync) {
            active.flush();
        } else {
            active.drain();
        }
        dirty = false;
    }

    @Override
    public void close() throws IOException {
        for (Segment s : segments) {
            s.close();
        }
    }
}
