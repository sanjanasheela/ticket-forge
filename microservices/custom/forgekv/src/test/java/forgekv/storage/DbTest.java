package forgekv.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import forgekv.util.Bytes;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DbTest {
    private static Bytes b(String s) {
        return Bytes.of(s);
    }

    private static Value s(String v) {
        return new Value.Str(b(v));
    }

    private static final long NONE = Entry.NO_EXPIRY;

    @Test
    void setGetRemove() {
        Db db = new Db();
        db.set(b("k"), s("v"), NONE);
        assertEquals(s("v"), db.get(b("k"), 0).value());
        assertNotNull(db.remove(b("k")));
        assertNull(db.get(b("k"), 0));
    }

    @Test
    void lazyExpiry() {
        Db db = new Db();
        db.set(b("k"), s("v"), 100);
        assertNotNull(db.get(b("k"), 99));
        assertNull(db.get(b("k"), 100));
        assertEquals(0, db.size());
        assertEquals(0, db.expiresSize());
        assertEquals(1, db.stats().expiredKeys());
    }

    @Test
    void setWithoutTtlClearsTtl() {
        Db db = new Db();
        db.set(b("k"), s("v"), 100);
        db.set(b("k"), s("v2"), NONE);
        assertEquals(0, db.expiresSize());
        assertNotNull(db.get(b("k"), 1_000));
    }

    @Test
    void keepTtl() {
        Db db = new Db();
        db.set(b("k"), s("v"), 100);
        db.setKeepTtl(b("k"), s("v2"), 50);
        assertEquals(50, db.ttlMs(b("k"), 50));
        assertNull(db.get(b("k"), 100));
    }

    @Test
    void expiryManagement() {
        Db db = new Db();
        assertFalse(db.setExpiry(b("nope"), 5, 0));
        db.set(b("k"), s("v"), NONE);
        assertEquals(Db.TTL_NONE, db.ttlMs(b("k"), 0));
        assertTrue(db.setExpiry(b("k"), 500, 0));
        assertEquals(400, db.ttlMs(b("k"), 100));
        assertTrue(db.setExpiry(b("k"), NONE, 100));
        assertEquals(0, db.expiresSize());
        assertEquals(Db.TTL_MISSING, db.ttlMs(b("missing"), 0));
    }

    @Test
    void activeExpiryReclaimsEverythingExpired() {
        Db db = new Db();
        for (int i = 0; i < 10_000; i++) {
            db.set(b("k" + i), s("v"), i % 2 == 0 ? 10 : 1_000_000);
        }
        for (int i = 0; i < 10_000; i++) {
            db.set(b("p" + i), s("v"), NONE);
        }
        int reclaimed = 0;
        for (int i = 0; i < 1_000; i++) {
            reclaimed += db.activeExpireCycle(20);
        }
        // Adaptive sampling stops once < 25% of samples are stale, so not
        // *every* key is reclaimed, but the vast majority must be.
        assertTrue(reclaimed > 4_000, "reclaimed only " + reclaimed);
        assertEquals(20_000 - reclaimed, db.size());
        assertEquals(10_000 - reclaimed, db.expiresSize());
        // Persistent keys must never be touched.
        for (int i = 0; i < 10_000; i++) {
            assertTrue(db.contains(b("p" + i), 20));
        }
    }

    @Test
    void passiveExpiryHidesButKeeps() {
        Db db = new Db();
        db.setPassiveExpiry(true);
        db.set(b("k"), s("v"), 100);
        assertNull(db.get(b("k"), 100));
        assertFalse(db.contains(b("k"), 100));
        assertEquals(Db.TTL_MISSING, db.ttlMs(b("k"), 100));
        assertEquals(0, db.activeExpireCycle(1_000));
        assertEquals(1, db.size(), "replica never deletes on its own");
        assertNotNull(db.get(b("k"), 50), "still readable before the deadline");
        db.setPassiveExpiry(false);
        assertNull(db.get(b("k"), 100));
        assertEquals(0, db.size());
    }

    @Test
    void removeWhereAndDrain() {
        Db db = new Db();
        for (String k : List.of("a1", "a2", "b1")) {
            db.set(b(k), s("v"), 10);
        }
        assertEquals(2, db.removeWhere(k -> k.startsWith(new byte[] {'a'})));
        assertEquals(1, db.expiresSize());
        List<Map.Entry<Bytes, Entry>> moved = db.drain();
        assertEquals(1, moved.size());
        assertTrue(db.isEmpty() && db.expiresSize() == 0);
        Db other = new Db();
        for (Map.Entry<Bytes, Entry> e : moved) {
            other.insertEntry(e.getKey(), e.getValue());
        }
        assertEquals(10, other.ttlMs(b("b1"), 0));
    }

    @Test
    void expireIndexSwapRemoveConsistency() {
        ExpireIndex idx = new ExpireIndex();
        for (String k : List.of("a", "b", "c", "d")) {
            idx.insert(b(k));
        }
        idx.insert(b("a")); // duplicate is a no-op
        assertEquals(4, idx.size());
        idx.remove(b("a"));
        idx.remove(b("zzz"));
        assertEquals(3, idx.size());
        for (int i = 0; i < idx.keys.size(); i++) {
            assertEquals(i, idx.pos.get(idx.keys.get(i)));
        }
    }
}
