package forgekv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import forgekv.resp.Frame;
import forgekv.server.Config;
import forgekv.server.Server;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

/**
 * Leader–follower replication, end to end.
 *
 * <p>Leader and replicas deliberately use <em>different</em> shard counts, so
 * the replicated stream has to be re-routed on the replica side.
 */
class ReplicationTest {
    private static Server leader(int shards) throws IOException {
        return Server.startLocal(Config.defaults().withShards(shards));
    }

    private static Server replicaOf(Server leader, int shards) throws IOException {
        return Server.startLocal(Config.defaults().withShards(shards).withReplicaof("127.0.0.1", leader.port()));
    }

    private static void eventually(String what, BooleanSupplier cond) throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (!cond.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("timed out waiting for: " + what);
            }
            Thread.sleep(20);
        }
    }

    private static void waitReplicas(RespClient c, long n) {
        assertEquals(n, c.integer("WAIT", n, 5_000), "WAIT " + n);
    }

    /** Every key, its type-specific contents and TTL presence. */
    private static Map<String, String> dump(RespClient c) {
        Map<String, String> out = new HashMap<>();
        for (String k : c.list("KEYS", "*")) {
            String t = c.str("TYPE", k);
            String v;
            switch (t) {
                case "string" -> v = c.str("GET", k);
                case "list" -> v = String.join(",", c.list("LRANGE", k, 0, -1));
                case "hash" -> {
                    List<String> pairs = new ArrayList<>();
                    c.hgetall(k).forEach((f, x) -> pairs.add(f + "=" + x));
                    Collections.sort(pairs);
                    v = pairs.toString();
                }
                case "set" -> {
                    List<String> s = new ArrayList<>(c.list("SMEMBERS", k));
                    Collections.sort(s);
                    v = String.join(",", s);
                }
                default -> {
                    continue;
                }
            }
            boolean hasTtl = c.integer("PTTL", k) > 0;
            out.put(k, t + "|" + v + "|ttl=" + hasTtl);
        }
        return out;
    }

    @Test
    void replicaMirrorsLeaderAndRejectsWrites() throws Exception {
        try (Server l = leader(2); RespClient lc = new RespClient(l.port())) {
            // Data that exists before the replica attaches (full sync)...
            lc.ok("SET", "greeting", "hello");
            lc.integer("RPUSH", "queue", "a", "b", "c");
            lc.ok("SET", "session", "s1", "EX", 300);

            try (Server r = replicaOf(l, 3); RespClient rc = new RespClient(r.port())) {
                eventually("link up", () -> "up".equals(rc.info("master_link_status")));

                // ...and data written afterwards (streamed).
                lc.integer("HSET", "profile", "name", "Ada", "tier", "gold");
                lc.integer("SADD", "tags", "x", "y");
                lc.ok("SET", "stock", 10);
                for (int i = 0; i < 3; i++) {
                    lc.integer("DECRIF", "stock", 0, 3);
                }
                assertNull(lc.integer("DECRIF", "stock", 0, 3)); // refused
                lc.str("LPOP", "queue");
                lc.integer("DEL", "greeting");
                waitReplicas(lc, 1);

                assertEquals(dump(lc), dump(rc));
                assertEquals("1", rc.str("GET", "stock"));
                assertTrue(rc.integer("TTL", "session") > 290, "absolute deadline replicated");

                Frame err = rc.call("SET", "nope", 1);
                assertTrue(err instanceof Frame.Error e && e.message().startsWith("READONLY"), err.toString());
                assertEquals("slave", rc.info("role"));
                assertEquals("1", lc.info("connected_slaves"));
                long lo = Long.parseLong(lc.info("master_repl_offset"));
                eventually("replica offset catches up", () -> Objects.equals(rc.info("slave_repl_offset"),
                        Long.toString(lo)));
            }
        }
    }

    @Test
    void expiryIsDrivenByTheLeader() throws Exception {
        try (Server l = leader(1); Server r = replicaOf(l, 2);
                RespClient lc = new RespClient(l.port()); RespClient rc = new RespClient(r.port())) {
            eventually("link up", () -> "up".equals(rc.info("master_link_status")));
            lc.ok("SET", "flash", "v", "PX", 150);
            waitReplicas(lc, 1);
            assertEquals("v", rc.str("GET", "flash"));
            Thread.sleep(250);
            // Hidden on the replica as soon as the deadline passes...
            assertNull(rc.str("GET", "flash"));
            // ...and physically removed once the leader's DEL arrives.
            eventually("leader DEL replicated", () -> rc.integer("DBSIZE") == 0);
        }
    }

    @Test
    void fullSyncUnderConcurrentWritesIsExact() throws Exception {
        try (Server l = leader(4); RespClient lc = new RespClient(l.port())) {
            int port = l.port();
            for (int i = 0; i < 2_000; i++) {
                lc.ok("SET", "pre:" + i, i);
            }
            AtomicBoolean stop = new AtomicBoolean();
            List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
            List<Thread> writers = new ArrayList<>();
            for (int w = 0; w < 4; w++) {
                int wi = w;
                Thread t = new Thread(() -> {
                    try (RespClient c = new RespClient(port)) {
                        long i = 0;
                        while (!stop.get()) {
                            List<Object[]> pipe = new ArrayList<>();
                            for (int k = 0; k < 16; k++) {
                                // RPUSH is not idempotent: a write applied twice (or
                                // lost) during the snapshot hand-over would show.
                                pipe.add(new Object[] {"RPUSH", "list:" + wi + ":" + k, i});
                                pipe.add(new Object[] {"INCR", "ctr:" + k});
                            }
                            c.pipeline(pipe).forEach(RespClient::check);
                            i++;
                        }
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                });
                writers.add(t);
                t.start();
            }

            Thread.sleep(100);
            try (Server r = replicaOf(l, 3); RespClient rc = new RespClient(r.port())) { // attaches mid-stream
                eventually("link up", () -> "up".equals(rc.info("master_link_status")));
                Thread.sleep(200);
                stop.set(true);
                for (Thread t : writers) {
                    t.join();
                }
                assertTrue(failures.isEmpty(), failures.toString());
                waitReplicas(lc, 1);

                Map<String, String> want = dump(lc);
                Map<String, String> got = dump(rc);
                assertEquals(want.size(), got.size());
                for (Map.Entry<String, String> e : want.entrySet()) {
                    assertEquals(e.getValue(), got.get(e.getKey()), e.getKey());
                }
            }
        }
    }

    @Test
    void reconnectResumesFromBacklog() throws Exception {
        try (Server l = leader(2); Server r = replicaOf(l, 2);
                RespClient lc = new RespClient(l.port()); RespClient rc = new RespClient(r.port())) {
            eventually("link up", () -> "up".equals(rc.info("master_link_status")));
            lc.ok("SET", "a", 1);
            waitReplicas(lc, 1);

            assertEquals(1, lc.integer("CLIENT", "KILL", "TYPE", "replica"));
            // Written while the replica is disconnected.
            for (int i = 0; i < 100; i++) {
                lc.integer("INCR", "during-outage");
                lc.integer("RPUSH", "outage-log", i);
            }
            eventually("replica reconnected and caught up", () -> "100".equals(rc.str("GET", "during-outage")));
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                expected.add(Integer.toString(i));
            }
            assertEquals(expected, rc.list("LRANGE", "outage-log", 0, -1));
            assertEquals("1", lc.info("sync_full"), "no second full sync");
            assertEquals("1", lc.info("sync_partial_ok"));
        }
    }

    @Test
    void flushallReplicatesAcrossDifferentShardCounts() throws Exception {
        try (Server l = leader(3); Server r = replicaOf(l, 2);
                RespClient lc = new RespClient(l.port()); RespClient rc = new RespClient(r.port())) {
            eventually("link up", () -> "up".equals(rc.info("master_link_status")));
            for (int i = 0; i < 300; i++) {
                lc.ok("SET", "old:" + i, i);
            }
            lc.ok("FLUSHALL");
            for (int i = 0; i < 300; i++) {
                lc.ok("SET", "new:" + i, i);
            }
            waitReplicas(lc, 1);
            assertEquals(300, rc.integer("DBSIZE"));
            assertTrue(rc.list("KEYS", "old:*").isEmpty());
        }
    }

    @Test
    void waitTimesOutWithoutReplicasAndPromotionAllowsWrites() throws Exception {
        try (Server l = leader(1); RespClient lc = new RespClient(l.port())) {
            lc.ok("SET", "k", 1);
            long t = System.nanoTime();
            assertEquals(0, lc.integer("WAIT", 1, 150));
            assertTrue(System.nanoTime() - t >= 140_000_000L);

            try (Server r = replicaOf(l, 1); RespClient rc = new RespClient(r.port())) {
                eventually("synced", () -> "1".equals(rc.str("GET", "k")));
                assertTrue(rc.call("WAIT", 0, 0) instanceof Frame.Error, "WAIT is refused on a replica");

                rc.ok("REPLICAOF", "NO", "ONE");
                assertEquals("master", rc.info("role"));
                rc.ok("SET", "k", 2);
                assertEquals("2", rc.str("GET", "k"));
                // Re-attach at runtime: a full resync brings it back in line.
                rc.ok("REPLICAOF", "127.0.0.1", l.port());
                eventually("re-synced from leader", () -> "1".equals(rc.str("GET", "k")));
            }
        }
    }
}
