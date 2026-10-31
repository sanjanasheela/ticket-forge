package forgelsm;

import java.util.BitSet;

final class Bloom {
    final BitSet bits; final int m, k;
    Bloom(int expected) { m = Math.max(64, expected * 10); k = 7; bits = new BitSet(m); }
    private Bloom(BitSet b, int m, int k) { bits = b; this.m = m; this.k = k; }
    static int h(String s, int seed) { int h = seed; for (int i = 0; i < s.length(); i++) h = h * 31 + s.charAt(i); h ^= h >>> 16; h *= 0x85ebca6b; h ^= h >>> 13; return h; }
    void add(String key) { int a = h(key, 17), b = h(key, 91); for (int i = 0; i < k; i++) bits.set(Math.floorMod(a + i * b, m)); }
    boolean mayContain(String key) { int a = h(key, 17), b = h(key, 91); for (int i = 0; i < k; i++) if (!bits.get(Math.floorMod(a + i * b, m))) return false; return true; }
    byte[] toBytes() { byte[] x = bits.toByteArray(); java.nio.ByteBuffer bb = java.nio.ByteBuffer.allocate(12 + x.length); bb.putInt(m).putInt(k).putInt(x.length).put(x); return bb.array(); }
    static Bloom from(java.nio.ByteBuffer bb) { int m = bb.getInt(), k = bb.getInt(), n = bb.getInt(); byte[] x = new byte[n]; bb.get(x); return new Bloom(BitSet.valueOf(x), m, k); }
}
