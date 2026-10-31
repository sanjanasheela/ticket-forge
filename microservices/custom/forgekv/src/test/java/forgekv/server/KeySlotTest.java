package forgekv.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class KeySlotTest {
    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void crc16MatchesReferenceVector() {
        assertEquals(0x31C3, KeySlot.crc16(b("123456789")));
    }

    @Test
    void slotsMatchRedisCluster() {
        // Values from `redis-cli CLUSTER KEYSLOT`.
        assertEquals(12182, KeySlot.keySlot(b("foo")));
        assertEquals(5061, KeySlot.keySlot(b("bar")));
        assertEquals(866, KeySlot.keySlot(b("hello")));
    }

    @Test
    void hashTags() {
        assertEquals(KeySlot.keySlot(b("{user1000}.following")), KeySlot.keySlot(b("{user1000}.followers")));
        assertEquals(KeySlot.keySlot(b("{user1000}.following")), KeySlot.keySlot(b("user1000")));
        // Empty tag => whole key is hashed.
        assertEquals(KeySlot.crc16(b("foo{}{bar}")) & (KeySlot.SLOTS - 1), KeySlot.keySlot(b("foo{}{bar}")));
        // Only the first tag counts.
        assertEquals(KeySlot.keySlot(b("{bar")), KeySlot.keySlot(b("foo{{bar}}zap")));
        // Unterminated tag => whole key.
        assertEquals(KeySlot.crc16(b("foo{bar")) & (KeySlot.SLOTS - 1), KeySlot.keySlot(b("foo{bar")));
    }

    @Test
    void shardsCoverAllSlotsEvenly() {
        for (int n : new int[] {1, 2, 3, 4, 7, 8, 12}) {
            int[] counts = new int[n];
            for (int i = 0; i < 100_000; i++) {
                counts[KeySlot.shardOf(b("key:" + i), n)]++;
            }
            int expected = 100_000 / n;
            for (int c : counts) {
                assertTrue(Math.abs(c - expected) < expected / 10, "n=" + n + ": " + c + " vs " + expected);
            }
        }
    }
}
