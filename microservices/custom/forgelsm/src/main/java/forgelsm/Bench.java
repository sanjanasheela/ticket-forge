package forgelsm;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public final class Bench {
    public static void main(String[] a) throws Exception {
        int n = a.length > 0 ? Integer.parseInt(a[0]) : 200_000;
        for (boolean fsync : new boolean[]{false, true}) {
            int ops = fsync ? Math.min(n, 3000) : n;
            Path d = Files.createTempDirectory("lsm");
            try (Lsm db = new Lsm(d, fsync, 4 << 20, 4)) {
                byte[] v = new byte[100];
                long t = System.nanoTime();
                for (int i = 0; i < ops; i++) db.put(String.format("k%09d", i), v);
                double w = ops / ((System.nanoTime() - t) / 1e9);
                Random r = new Random(1); t = System.nanoTime(); int hit = 0; int reads = Math.min(ops, 50_000);
                for (int i = 0; i < reads; i++) if (db.get(String.format("k%09d", r.nextInt(ops))) != null) hit++;
                double rd = reads / ((System.nanoTime() - t) / 1e9);
                t = System.nanoTime();
                for (int i = 0; i < reads; i++) db.get("miss" + i);
                double miss = reads / ((System.nanoTime() - t) / 1e9);
                System.out.printf("{\"fsync\":%b,\"writes\":%d,\"writes_per_s\":%.0f,\"reads_per_s\":%.0f,\"hits\":%d,\"miss_reads_per_s\":%.0f,\"flushes\":%d,\"compactions\":%d,\"tables\":%d}%n",
                        fsync, ops, w, rd, hit, miss, db.flushes, db.compactions, db.tableCount());
            }
        }
    }
}
