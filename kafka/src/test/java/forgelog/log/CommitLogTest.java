package forgelog.log;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CommitLogTest {
    private static final CommitLog.Config SMALL = new CommitLog.Config(4096, 64);

    @TempDir
    Path dir;

    private static Record rec(long offset, long term, String value) {
        return new Record(offset, term, Record.DATA, 1000 + offset, 42, (int) offset, ("k" + offset).getBytes(),
                value.getBytes(StandardCharsets.UTF_8));
    }

    /** Terms change every 37 records, so term runs straddle segment boundaries. */
    private static long termFor(long offset) {
        return 1 + offset / 37;
    }

    private static void fill(CommitLog log, int n) {
        for (long i = log.nextOffset(); i < n; i++) {
            log.append(rec(i, termFor(i), "value-" + i));
        }
    }

    private Path lastSegment() throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.toString().endsWith(".log")).sorted().toList().getLast();
        }
    }

    @Test
    void recordRoundTripAndCrcCheck() throws Exception {
        Record r = rec(5, 2, "hello");
        ByteBuffer b = r.toBuffer();
        assertEquals(r, Record.decode(b.duplicate()));
        b.put(30, (byte) (b.get(30) ^ 1));
        assertThrows(CorruptRecordException.class, () -> Record.decode(b));
        Record nulls = new Record(0, 1, Record.CONTROL, 0, -1, -1, null, null);
        assertEquals(nulls, Record.decode(nulls.toBuffer()));
        assertNull(Record.decode(ByteBuffer.wrap(new byte[] {0, 0})), "partial record");
    }

    @Test
    void appendsRollSegmentsAndSurviveReopen() throws IOException {
        try (CommitLog log = CommitLog.open(dir, SMALL)) {
            fill(log, 1000);
            assertTrue(log.segmentCount() > 10, "small segments roll: " + log.segmentCount());
            assertEquals(999, log.lastIndex());
            log.flush(true);
        }
        try (CommitLog log = CommitLog.open(dir, SMALL)) {
            assertEquals(999, log.lastIndex());
            for (long i = 0; i < 1000; i++) {
                assertEquals(termFor(i), log.termAt(i), "term of " + i);
                assertEquals(termFor(i) * 37 - 37, log.firstIndexOfTerm(i));
                assertEquals(rec(i, termFor(i), "value-" + i), log.read(i));
            }
            assertEquals(0, log.termAt(-1));
            assertEquals(-1, log.termAt(1000));
            // Batched reads stop at segment boundaries and at maxBytes, but never return nothing.
            long next = 0;
            while (next <= log.lastIndex()) {
                List<Record> batch = log.entries(next, log.lastIndex(), 500);
                assertTrue(!batch.isEmpty());
                assertEquals(next, batch.getFirst().offset);
                next = batch.getLast().offset + 1;
            }
            fill(log, 1100);
            assertEquals(rec(1050, termFor(1050), "value-1050"), log.read(1050));
        }
    }

    @Test
    void tornTailIsTruncatedOnRecovery() throws IOException {
        try (CommitLog log = CommitLog.open(dir, SMALL)) {
            fill(log, 100);
            log.flush(true);
        }
        Path seg = lastSegment();
        long goodSize = Files.size(seg);
        ByteBuffer half = rec(100, termFor(100), "half-written").toBuffer();
        half.limit(half.limit() / 2);
        try (FileChannel ch = FileChannel.open(seg, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ch.write(half);
        }
        try (CommitLog log = CommitLog.open(dir, SMALL)) {
            assertEquals(99, log.lastIndex());
            assertEquals(goodSize, Files.size(seg));
            log.append(rec(100, termFor(100), "rewritten"));
            assertEquals("rewritten", new String(log.read(100).value));
        }
    }

    @Test
    void corruptRecordInActiveSegmentTruncatesFromThere() throws IOException {
        long corruptAt;
        try (CommitLog log = CommitLog.open(dir, SMALL)) {
            fill(log, 100);
            log.flush(true);
            corruptAt = log.lastIndex() - 3;
        }
        Path seg = lastSegment();
        try (CommitLog log = CommitLog.open(dir, SMALL)) {
            Region r = log.region(corruptAt, corruptAt + 1, 1 << 20);
            try (FileChannel ch = FileChannel.open(seg, StandardOpenOption.WRITE)) {
                ch.write(ByteBuffer.wrap(new byte[] {(byte) 0xFF}), r.position() + 40);
            }
        }
        try (CommitLog log = CommitLog.open(dir, SMALL)) {
            assertEquals(corruptAt - 1, log.lastIndex(), "everything from the bad record on is dropped");
        }
    }

    @Test
    void truncateAcrossSegmentsThenAppendNewTerm() throws IOException {
        try (CommitLog log = CommitLog.open(dir, SMALL)) {
            fill(log, 500);
            int before = log.segmentCount();
            log.truncateFrom(123);
            assertEquals(122, log.lastIndex());
            assertTrue(log.segmentCount() < before);
            assertEquals(termFor(122), log.termAt(122));
            for (long i = 123; i < 200; i++) {
                log.append(rec(i, 99, "new-" + i));
            }
            assertEquals(99, log.termAt(150));
            assertEquals(123, log.firstIndexOfTerm(199));
            log.flush(true);
        }
        try (CommitLog log = CommitLog.open(dir, SMALL)) {
            assertEquals(199, log.lastIndex());
            assertEquals("new-150", new String(log.read(150).value));
            assertEquals(123, log.firstIndexOfTerm(150));
            assertEquals(termFor(100), log.termAt(100));
        }
    }

    @Test
    void zeroCopyRegionHoldsExactlyTheEncodedRecords() throws IOException {
        try (CommitLog log = CommitLog.open(dir, new CommitLog.Config(1 << 20, 1024))) {
            fill(log, 50);
            Region r = log.region(10, 20, 1 << 20);
            assertEquals(20, r.endOffset());
            ByteBuffer expected = ByteBuffer.allocate(r.length());
            for (long i = 10; i < 20; i++) {
                rec(i, termFor(i), "value-" + i).encode(expected);
            }
            ByteBuffer actual = ByteBuffer.allocate(r.length());
            r.channel().read(actual, r.position());
            assertArrayEquals(expected.array(), actual.array());

            Region small = log.region(10, 50, 200);
            assertTrue(small.endOffset() > 10 && small.endOffset() < 50, "maxBytes respected");
            assertNull(log.region(50, 50, 100), "nothing past the end");
            Region one = log.region(0, 50, 1);
            assertEquals(1, one.endOffset(), "at least one record even if larger than maxBytes");
        }
    }

    @Test
    void rejectsOutOfOrderAppends() throws IOException {
        try (CommitLog log = CommitLog.open(dir, SMALL)) {
            log.append(rec(0, 1, "a"));
            assertThrows(IllegalArgumentException.class, () -> log.append(rec(5, 1, "b")));
        }
    }
}
