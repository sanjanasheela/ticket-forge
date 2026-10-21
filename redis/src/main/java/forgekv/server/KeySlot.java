package forgekv.server;

import forgekv.util.Bytes;

/**
 * Key → shard mapping.
 *
 * <p>A key is hashed with CRC16/XMODEM into one of 16384 slots (exactly the
 * function Redis Cluster uses) and slots are split into {@code n} contiguous
 * ranges, one per shard. Hash tags work as in Redis Cluster: only the part
 * between the first <code>{</code> and the next <code>}</code> is hashed, so
 * <code>{event:7}:stock</code> and <code>{event:7}:lock</code> always live on
 * the same shard and can be used together in one atomic multi-key command.
 */
public final class KeySlot {
    public static final int SLOTS = 16384;

    private static final int[] CRC16 = table();

    private KeySlot() {}

    private static int[] table() {
        int[] t = new int[256];
        for (int i = 0; i < 256; i++) {
            int crc = i << 8;
            for (int bit = 0; bit < 8; bit++) {
                crc = (crc & 0x8000) != 0 ? ((crc << 1) ^ 0x1021) : (crc << 1);
                crc &= 0xFFFF;
            }
            t[i] = crc;
        }
        return t;
    }

    static int crc16(byte[] data, int off, int len) {
        int crc = 0;
        for (int i = off; i < off + len; i++) {
            crc = ((crc << 8) ^ CRC16[((crc >>> 8) ^ (data[i] & 0xFF)) & 0xFF]) & 0xFFFF;
        }
        return crc;
    }

    static int crc16(byte[] data) {
        return crc16(data, 0, data.length);
    }

    /** Redis Cluster slot of {@code key}: the {tag} content if non-empty, else the key. */
    public static int keySlot(byte[] key) {
        for (int open = 0; open < key.length; open++) {
            if (key[open] == '{') {
                for (int close = open + 1; close < key.length; close++) {
                    if (key[close] == '}') {
                        if (close > open + 1) {
                            return crc16(key, open + 1, close - open - 1) & (SLOTS - 1);
                        }
                        break;
                    }
                }
                break;
            }
        }
        return crc16(key) & (SLOTS - 1);
    }

    public static int keySlot(Bytes key) {
        return keySlot(key.array());
    }

    /** Index of the shard owning {@code key} among {@code n} shards. */
    public static int shardOf(Bytes key, int n) {
        return n <= 1 ? 0 : keySlot(key.array()) * n / SLOTS;
    }

    public static int shardOf(byte[] key, int n) {
        return n <= 1 ? 0 : keySlot(key) * n / SLOTS;
    }
}
