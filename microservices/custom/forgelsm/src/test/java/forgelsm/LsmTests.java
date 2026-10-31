package forgelsm;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class LsmTests {
    @TempDir Path dir;
    static byte[] b(String s) { return s.getBytes(); }

    @Test void putGetDeleteOverwrite() throws Exception {
        try (Lsm db = new Lsm(dir, false, 1 << 20, 4)) {
            db.put("a", b("1")); db.put("b", b("2")); db.put("a", b("3"));
            assertEquals("3", new String(db.get("a"))); db.delete("b");
            assertNull(db.get("b")); assertNull(db.get("zzz"));
        }
    }

    @Test void flushAndCompactionKeepNewestAndDropTombstones() throws Exception {
        try (Lsm db = new Lsm(dir, false, 2000, 3)) {
            for (int round = 0; round < 6; round++) for (int i = 0; i < 200; i++) db.put("k" + i, b("r" + round));
            for (int i = 0; i < 200; i += 2) db.delete("k" + i);
            db.compact();
            assertTrue(db.flushes > 0 && db.compactions > 0);
            assertEquals(1, db.tableCount());
            for (int i = 0; i < 200; i++) {
                if (i % 2 == 0) assertNull(db.get("k" + i), "k" + i); else assertEquals("r5", new String(db.get("k" + i)));
            }
        }
    }

    @Test void recoversFromWalAfterCrashWithoutClose() throws Exception {
        Lsm db = new Lsm(dir, true, 1 << 20, 4);          // never closed = crash
        for (int i = 0; i < 500; i++) db.put("k" + i, b("v" + i));
        db.delete("k7");
        try (Lsm re = new Lsm(dir, true, 1 << 20, 4)) {
            assertEquals("v499", new String(re.get("k499"))); assertNull(re.get("k7")); assertEquals("v8", new String(re.get("k8")));
        }
    }

    @Test void tornWalTailIsIgnored() throws Exception {
        Lsm db = new Lsm(dir, true, 1 << 20, 4);
        db.put("good", b("1")); db.put("also", b("2"));
        Path w = dir.resolve("wal.log"); byte[] all = Files.readAllBytes(w);
        Files.write(w, Arrays.copyOf(all, all.length - 3));          // chop the last record
        try (Lsm re = new Lsm(dir, true, 1 << 20, 4)) { assertEquals("1", new String(re.get("good"))); assertNull(re.get("also")); }
    }

    @Test void reopenReadsFlushedTablesAndRange() throws Exception {
        try (Lsm db = new Lsm(dir, false, 1000, 100)) { for (int i = 0; i < 300; i++) db.put(String.format("k%03d", i), b("v" + i)); }
        try (Lsm db = new Lsm(dir, false, 1000, 100)) {
            assertEquals("v123", new String(db.get("k123")));
            var r = db.range("k010", "k020"); assertEquals(10, r.size()); assertEquals("k010", r.firstKey());
        }
    }

    @Test void bloomHasNoFalseNegatives() {
        Bloom bl = new Bloom(5000); for (int i = 0; i < 5000; i++) bl.add("x" + i);
        for (int i = 0; i < 5000; i++) assertTrue(bl.mayContain("x" + i));
        int fp = 0; for (int i = 0; i < 5000; i++) if (bl.mayContain("y" + i)) fp++;
        assertTrue(fp < 250, "fp=" + fp);
    }
}
