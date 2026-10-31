package forgelsm;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

/** LSM-tree store: WAL -> memtable -> flushed SSTables -> size-tiered compaction. Keys are strings, values bytes. */
public final class Lsm implements Closeable {
    private final Path dir; private final boolean fsync; private final long memLimit; private final int compactAt;
    private ConcurrentSkipListMap<String, byte[]> mem = new ConcurrentSkipListMap<>();
    private long memBytes; private long seq;
    private final List<SSTable> tables = new ArrayList<>();        // oldest first
    private DataOutputStream wal; private FileOutputStream walFile;
    private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock();
    public long flushes, compactions;

    public Lsm(Path dir, boolean fsync, long memLimitBytes, int compactAt) throws IOException {
        this.dir = dir; this.fsync = fsync; this.memLimit = memLimitBytes; this.compactAt = compactAt;
        Files.createDirectories(dir);
        try (var s = Files.list(dir)) {
            for (Path p : (Iterable<Path>) s.sorted()::iterator) {
                String n = p.getFileName().toString();
                if (n.endsWith(".tmp")) Files.delete(p);
                else if (n.startsWith("sst-") && n.endsWith(".dat")) { long q = Long.parseLong(n.substring(4, n.length() - 4)); tables.add(SSTable.open(p, q)); seq = Math.max(seq, q); }
            }
        }
        replayWal(); openWal();
    }

    // ---- WAL ----
    private Path walPath() { return dir.resolve("wal.log"); }
    private void openWal() throws IOException { walFile = new FileOutputStream(walPath().toFile(), true); wal = new DataOutputStream(new BufferedOutputStream(walFile)); }
    private void replayWal() throws IOException {
        Path p = walPath(); if (!Files.exists(p)) return;
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(p)))) {
            while (true) {
                int crc, kl, vl; byte[] k, v;
                try { crc = in.readInt(); kl = in.readInt(); vl = in.readInt(); k = in.readNBytes(kl); v = vl < 0 ? SSTable.TOMB : in.readNBytes(vl); }
                catch (EOFException e) { break; }
                if (k.length != kl || (vl >= 0 && v.length != vl) || crc != crc(k, vl, v)) break;          // torn tail: stop
                mem.put(new String(k, StandardCharsets.UTF_8), v); memBytes += kl + Math.max(vl, 0);
            }
        }
    }
    private static int crc(byte[] k, int vl, byte[] v) { CRC32 c = new CRC32(); c.update(k); c.update(vl); c.update(v); return (int) c.getValue(); }

    // ---- API ----
    public void put(String key, byte[] value) throws IOException { write(key, value); }
    public void delete(String key) throws IOException { write(key, SSTable.TOMB); }

    private void write(String key, byte[] v) throws IOException {
        rw.writeLock().lock();
        try {
            byte[] k = key.getBytes(StandardCharsets.UTF_8); int vl = v == SSTable.TOMB ? -1 : v.length;
            wal.writeInt(crc(k, vl, v == SSTable.TOMB ? new byte[0] : v)); wal.writeInt(k.length); wal.writeInt(vl); wal.write(k); if (vl >= 0) wal.write(v);
            wal.flush(); if (fsync) walFile.getFD().sync();
            mem.put(key, v); memBytes += k.length + Math.max(vl, 0);
            if (memBytes >= memLimit) flush();
        } finally { rw.writeLock().unlock(); }
    }

    public byte[] get(String key) throws IOException {
        rw.readLock().lock();
        try {
            byte[] v = mem.get(key);
            if (v == null) for (int i = tables.size() - 1; i >= 0 && v == null; i--) v = tables.get(i).get(key);
            return v == null || v == SSTable.TOMB ? null : v;
        } finally { rw.readLock().unlock(); }
    }

    /** Sorted merged view in [from, to). */
    public SortedMap<String, byte[]> range(String from, String to) throws IOException {
        rw.readLock().lock();
        try {
            TreeMap<String, byte[]> out = new TreeMap<>();
            for (SSTable t : tables) for (var it = t.scanAll(); it.hasNext(); ) { var e = it.next(); if (e.getKey().compareTo(from) >= 0 && e.getKey().compareTo(to) < 0) out.put(e.getKey(), e.getValue()); }
            for (var e : mem.subMap(from, to).entrySet()) out.put(e.getKey(), e.getValue());
            out.values().removeIf(v -> v == SSTable.TOMB);
            return out;
        } finally { rw.readLock().unlock(); }
    }

    public int tableCount() { return tables.size(); }

    private void flush() throws IOException {
        if (mem.isEmpty()) return;
        Path p = dir.resolve(String.format("sst-%08d.dat", ++seq));
        tables.add(SSTable.write(p, seq, mem.entrySet().iterator(), mem.size()));
        flushes++; mem = new ConcurrentSkipListMap<>(); memBytes = 0;
        wal.close(); Files.deleteIfExists(walPath()); openWal();
        if (tables.size() >= compactAt) compact();
    }

    /** Merge all tables into one; newer wins; tombstones are dropped because nothing older remains. */
    public void compact() throws IOException {
        rw.writeLock().lock();
        try {
            if (tables.size() < 2) return;
            TreeMap<String, byte[]> merged = new TreeMap<>();
            for (SSTable t : tables) for (var it = t.scanAll(); it.hasNext(); ) { var e = it.next(); merged.put(e.getKey(), e.getValue()); }
            merged.values().removeIf(v -> v == SSTable.TOMB);
            Path p = dir.resolve(String.format("sst-%08d.dat", ++seq));
            SSTable fresh = SSTable.write(p, seq, merged.entrySet().iterator(), merged.size());
            for (SSTable t : tables) { t.close(); Files.delete(t.path); }
            tables.clear(); tables.add(fresh); compactions++;
        } finally { rw.writeLock().unlock(); }
    }

    @Override public void close() throws IOException {
        rw.writeLock().lock();
        try { wal.flush(); wal.close(); for (SSTable t : tables) t.close(); } finally { rw.writeLock().unlock(); }
    }
}
