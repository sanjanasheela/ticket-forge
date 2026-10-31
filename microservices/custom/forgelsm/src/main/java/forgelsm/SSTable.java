package forgelsm;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Immutable sorted file. Layout: [records][sparse index][bloom][footer: indexPos, bloomPos, count, magic].
 * record = keyLen(int) key valLen(int, -1 = tombstone) val. Sparse index holds every 16th key's offset.
 */
final class SSTable {
    static final int EVERY = 16; static final long MAGIC = 0x46474C534D303031L;
    final Path path; final long seq; final TreeMap<String, Long> index = new TreeMap<>(); final Bloom bloom; final long dataEnd; final int count;
    final FileChannel ch;

    static final byte[] TOMB = new byte[0];

    private SSTable(Path p, long seq) throws IOException {
        path = p; this.seq = seq; ch = FileChannel.open(p, StandardOpenOption.READ);
        ByteBuffer f = ByteBuffer.allocate(32); ch.read(f, ch.size() - 32); f.flip();
        long ip = f.getLong(), bp = f.getLong(); count = (int) f.getLong(); if (f.getLong() != MAGIC) throw new IOException("bad sstable " + p);
        dataEnd = ip;
        ByteBuffer ib = ByteBuffer.allocate((int) (bp - ip)); ch.read(ib, ip); ib.flip();
        while (ib.hasRemaining()) { byte[] k = new byte[ib.getInt()]; ib.get(k); index.put(new String(k, StandardCharsets.UTF_8), ib.getLong()); }
        ByteBuffer bb = ByteBuffer.allocate((int) (ch.size() - 32 - bp)); ch.read(bb, bp); bb.flip(); bloom = Bloom.from(bb);
    }
    static SSTable open(Path p, long seq) throws IOException { return new SSTable(p, seq); }

    /** Writes a sorted run (values equal to TOMB identity are tombstones). */
    static SSTable write(Path p, long seq, Iterator<Map.Entry<String, byte[]>> it, int expected) throws IOException {
        Path tmp = Paths.get(p + ".tmp");
        Bloom bl = new Bloom(Math.max(expected, 1)); int n = 0;
        ByteArrayOutputStream idx = new ByteArrayOutputStream(); DataOutputStream di = new DataOutputStream(idx);
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(tmp)))) {
            while (it.hasNext()) {
                var e = it.next(); byte[] k = e.getKey().getBytes(StandardCharsets.UTF_8); byte[] v = e.getValue();
                if (n % EVERY == 0) { di.writeInt(k.length); di.write(k); di.writeLong(out.size()); }
                out.writeInt(k.length); out.write(k);
                if (v == TOMB) out.writeInt(-1); else { out.writeInt(v.length); out.write(v); }
                bl.add(e.getKey()); n++;
            }
            long ip = out.size(); out.write(idx.toByteArray());
            long bp = out.size(); out.write(bl.toBytes());
            out.writeLong(ip); out.writeLong(bp); out.writeLong(n); out.writeLong(MAGIC);
        }
        try (FileChannel c = FileChannel.open(tmp, StandardOpenOption.WRITE)) { c.force(true); }
        Files.move(tmp, p, StandardCopyOption.ATOMIC_MOVE);
        return open(p, seq);
    }

    /** null = not here; TOMB = deleted here; else value. */
    byte[] get(String key) throws IOException {
        if (!bloom.mayContain(key)) return null;
        var floor = index.floorEntry(key); if (floor == null) return null;
        long pos = floor.getValue(); var nx = index.higherEntry(floor.getKey());
        var it = scan(pos, nx == null ? dataEnd : nx.getValue());
        while (it.hasNext()) { var e = it.next(); int c = e.getKey().compareTo(key); if (c == 0) return e.getValue(); if (c > 0) return null; }
        return null;
    }

    Iterator<Map.Entry<String, byte[]>> scan(long from) throws IOException { return scan(from, dataEnd); }
    Iterator<Map.Entry<String, byte[]>> scan(long from, long to) throws IOException {
        ByteBuffer all = ByteBuffer.allocate((int) (to - from)); ch.read(all, from); all.flip();
        return new Iterator<>() {
            public boolean hasNext() { return all.hasRemaining(); }
            public Map.Entry<String, byte[]> next() {
                byte[] k = new byte[all.getInt()]; all.get(k); int vl = all.getInt(); byte[] v;
                if (vl < 0) v = TOMB; else { v = new byte[vl]; all.get(v); }
                return Map.entry(new String(k, StandardCharsets.UTF_8), v);
            }
        };
    }
    Iterator<Map.Entry<String, byte[]>> scanAll() throws IOException { return scan(0); }
    void close() throws IOException { ch.close(); }
}
