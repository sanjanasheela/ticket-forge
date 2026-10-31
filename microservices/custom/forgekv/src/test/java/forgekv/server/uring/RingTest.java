package forgekv.server.uring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RingTest {
    @TempDir
    Path dir;

    private static MemorySegment nativeBytes(Arena arena, String s) {
        byte[] b = s.getBytes(StandardCharsets.US_ASCII);
        MemorySegment seg = arena.allocate(b.length);
        seg.copyFrom(MemorySegment.ofArray(b));
        return seg;
    }

    @Test
    void linkedWriteThenFsync() throws IOException {
        Ring ring;
        try {
            ring = Ring.create(8);
        } catch (IOException | RuntimeException e) {
            System.err.println("io_uring unavailable, skipping: " + e);
            return;
        }
        Path path = dir.resolve("uring");
        int fd = Ring.openForWrite(path.toString());
        try (ring; Arena arena = Arena.ofConfined()) {
            MemorySegment a = nativeBytes(arena, "hello ");
            MemorySegment b = nativeBytes(arena, "io_uring");
            assertTrue(ring.pushWrite(fd, a, 0, Ring.SQE_IO_LINK, 1));
            assertTrue(ring.pushFsync(fd, 2));
            assertTrue(ring.pushWrite(fd, b, a.byteSize(), (byte) 0, 3));
            ring.submit(3);
            List<long[]> seen = new ArrayList<>();
            while (seen.size() < 3) {
                Ring.Cqe c = ring.pop();
                if (c != null) {
                    seen.add(new long[] {c.userData(), c.res()});
                } else {
                    ring.submit(1);
                }
            }
            seen.sort(Comparator.comparingLong(x -> x[0]));
            assertEquals(List.of("1:6", "2:0", "3:8"), seen.stream().map(x -> x[0] + ":" + x[1]).toList());
            assertEquals("hello io_uring", Files.readString(path));
        } finally {
            Ring.closeFd(fd);
        }
    }

    @Test
    void eventfdIsSignalledOnCompletion() throws IOException {
        Ring ring;
        try {
            ring = Ring.create(4);
        } catch (IOException | RuntimeException e) {
            return;
        }
        int efd = Ring.eventfd();
        Path path = dir.resolve("uring-efd");
        int fd = Ring.openForWrite(path.toString());
        try (ring) {
            ring.registerEventfd(efd);
            ring.pushFsync(fd, 7);
            ring.submit(1);
            long v = Ring.waitEventfd(efd);
            assertTrue(v >= 1);
            assertEquals(7, ring.pop().userData());
        } finally {
            Ring.closeFd(fd);
            Ring.closeFd(efd);
        }
    }
}
