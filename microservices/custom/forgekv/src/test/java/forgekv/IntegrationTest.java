package forgekv;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import forgekv.RespClient.RespError;
import forgekv.resp.Frame;
import forgekv.server.Config;
import forgekv.server.KeySlot;
import forgekv.server.Server;
import forgekv.server.aof.AofConfig;
import forgekv.server.aof.FsyncPolicy;
import forgekv.server.aof.IoBackend;
import forgekv.server.uring.Ring;
import forgekv.util.Bytes;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end tests: a real ForgeKV server driven over TCP.
 *
 * <p>The shared server runs <b>4 shards</b>, so every test below also
 * exercises cross-shard routing, multi-key splitting and reply re-ordering.
 */
class IntegrationTest {
    static final int SHARDS = 4;
    private static Server sharded;
    private static Server single;

    @TempDir
    Path tmpDir;

    private static synchronized int server() {
        if (sharded == null) {
            try {
                sharded = Server.startLocal(Config.defaults().withShards(SHARDS));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return sharded.port();
    }

    /** The classic single-threaded configuration. */
    private static synchronized int single() {
        if (single == null) {
            try {
                single = Server.startLocal(Config.defaults());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return single.port();
    }

    @AfterAll
    static void stopServers() {
        if (sharded != null) {
            sharded.close();
        }
        if (single != null) {
            single.close();
        }
    }

    private static RespClient conn() {
        return new RespClient(server());
    }

    @Test
    void pingEcho() {
        try (RespClient c = new RespClient(single())) {
            assertEquals("PONG", c.str("PING"));
        }
        try (RespClient c = conn()) {
            assertEquals("PONG", c.str("PING"));
            assertEquals("héllo", c.str("ECHO", "héllo"));
        }
    }

    @Test
    void stringsRoundtrip() {
        try (RespClient c = conn()) {
            c.ok("SET", "it:str", "value");
            assertEquals("value", c.str("GET", "it:str"));
            assertNull(c.str("GET", "it:nope"));

            assertEquals(5, c.integer("INCRBY", "it:counter", 5));
            c.ok("MSET", "it:a", "1", "it:b", "2");
            assertEquals(Arrays.asList("1", null, "2"), c.list("MGET", "it:a", "it:zz", "it:b"));

            assertEquals(2, c.integer("DEL", "it:a", "it:b", "it:zz"));
        }
    }

    @Test
    void binaryValues() {
        try (RespClient c = conn()) {
            byte[] blob = new byte[256];
            for (int i = 0; i < 256; i++) {
                blob[i] = (byte) i;
            }
            c.ok("SET", "it:bin", blob);
            assertArrayEquals(blob, c.bytes("GET", "it:bin"));
        }
    }

    @Test
    void ttlExpiry() throws Exception {
        try (RespClient c = conn()) {
            c.ok("SET", "it:ttl", "v", "PX", 150);
            long pttl = c.integer("PTTL", "it:ttl");
            assertTrue(pttl > 0 && pttl <= 150);
            Thread.sleep(200);
            assertNull(c.str("GET", "it:ttl"), "lazy expiry");
        }
    }

    @Test
    void activeExpiryReclaimsUntouchedKeys() throws Exception {
        try (RespClient c = conn()) {
            List<Object[]> pipe = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                pipe.add(new Object[] {"SET", "it:active:" + i, "v", "PX", 50});
            }
            c.pipeline(pipe).forEach(RespClient::check);
            // Never read the keys again: only the background cycle can reclaim them.
            Thread.sleep(1_500);
            long expired = Long.parseLong(c.info("expired_keys"));
            assertTrue(expired >= 500, "expired_keys=" + expired);
        }
    }

    @Test
    void pipeliningPreservesOrder() {
        try (RespClient c = conn()) {
            List<Object[]> pipe = new ArrayList<>();
            for (int i = 0; i < 1_000; i++) {
                pipe.add(new Object[] {"INCR", "it:pipe"});
            }
            List<Frame> results = c.pipeline(pipe);
            for (int i = 0; i < 1_000; i++) {
                assertEquals(Frame.integer(i + 1), results.get(i));
            }
        }
    }

    @Test
    void errorsAreReportedNotFatal() {
        try (RespClient c = conn()) {
            assertTrue(c.call("NOSUCHCMD") instanceof Frame.Error);
            c.ok("SET", "it:notint", "abc");
            RespError e = assertThrows(RespError.class, () -> c.integer("INCRBY", "it:notint", 1));
            assertTrue(e.getMessage().contains("not an integer"));
            // Connection must still be usable.
            assertEquals("PONG", c.str("PING"));
        }
    }

    @Test
    void distributedLockWithSetNxPxAndCad() {
        try (RespClient c = conn()) {
            assertEquals("OK", c.str("SET", "it:lock", "owner-1", "NX", "PX", 5_000));
            assertNull(c.str("SET", "it:lock", "owner-2", "NX", "PX", 5_000), "lock already held");
            assertEquals(0, c.integer("CAD", "it:lock", "owner-2"), "non-owner cannot release");
            assertEquals(1, c.integer("CAD", "it:lock", "owner-1"));
        }
    }

    /**
     * The TicketForge guarantee: N concurrent buyers racing for S seats never
     * sell more than S tickets.
     */
    @Test
    void flashSaleNeverOversells() throws Exception {
        flashSale(single());
        flashSale(server());
    }

    private static void flashSale(int port) throws Exception {
        final long stock = 1_000;
        final int buyers = 32;
        final int attemptsPerBuyer = 100;

        try (RespClient c = new RespClient(port)) {
            c.ok("SET", "it:stock", stock);
            AtomicLong sold = new AtomicLong();
            List<Thread> threads = new ArrayList<>();
            List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
            for (int b = 0; b < buyers; b++) {
                Thread t = new Thread(() -> {
                    try (RespClient bc = new RespClient(port)) {
                        for (int i = 0; i < attemptsPerBuyer; i++) {
                            if (bc.integer("DECRIF", "it:stock", 0) != null) {
                                sold.incrementAndGet();
                            }
                        }
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                });
                threads.add(t);
                t.start();
            }
            for (Thread t : threads) {
                t.join();
            }
            assertTrue(failures.isEmpty(), failures.toString());
            assertEquals(stock, sold.get(), "exactly STOCK tickets sold");
            assertEquals("0", c.str("GET", "it:stock"));
        }
    }

    // -----------------------------------------------------------------------
    // collections
    // -----------------------------------------------------------------------

    @Test
    void collectionsViaClient() {
        try (RespClient c = conn()) {
            assertEquals(3, c.integer("RPUSH", "it:list", "a", "b", "c"));
            assertEquals(List.of("a", "b", "c"), c.list("LRANGE", "it:list", 0, -1));
            assertEquals("a", c.str("LPOP", "it:list"));

            c.integer("HSET", "it:hash", "f1", "v1", "f2", "v2");
            Map<String, String> m = c.hgetall("it:hash");
            assertEquals(2, m.size());
            assertEquals("v2", m.get("f2"));

            c.integer("SADD", "it:set", "x", "y");
            assertEquals(1, c.integer("SISMEMBER", "it:set", "x"));
            assertEquals("set", c.str("TYPE", "it:set"));

            RespError err = assertThrows(RespError.class, () -> c.integer("LLEN", "it:set"));
            assertTrue(err.getMessage().contains("WRONGTYPE"));

            List<String> keys = c.list("KEYS", "it:*");
            assertTrue(keys.contains("it:list") && keys.contains("it:hash"));
        }
    }

    // -----------------------------------------------------------------------
    // pub/sub
    // -----------------------------------------------------------------------

    private static RespClient subscriber() {
        RespClient c = conn();
        c.setTimeout(3_000);
        return c;
    }

    /** A pushed {@code message}/{@code pmessage} as [kind, (pattern,) channel, payload]. */
    private static List<String> message(RespClient c) {
        return RespClient.asList(c.read());
    }

    @Test
    void pubsubChannelDelivery() {
        try (RespClient sub = subscriber(); RespClient p = conn()) {
            assertEquals(List.of("subscribe", "it:ps:chan", "1"), sub.list("SUBSCRIBE", "it:ps:chan"));
            assertEquals(1, p.integer("PUBLISH", "it:ps:chan", "hello"));
            assertEquals(0, p.integer("PUBLISH", "it:ps:nobody", "x"));
            assertEquals(List.of("message", "it:ps:chan", "hello"), message(sub));
        }
    }

    @Test
    void pubsubPatternAndFanout() {
        try (RespClient s1 = subscriber(); RespClient s2 = subscriber(); RespClient p = conn()) {
            s1.list("PSUBSCRIBE", "it:news:*");
            s2.list("SUBSCRIBE", "it:news:sports");
            // One pattern subscriber + one channel subscriber.
            assertEquals(2, p.integer("PUBLISH", "it:news:sports", "goal"));
            assertEquals(List.of("pmessage", "it:news:*", "it:news:sports", "goal"), message(s1));
            assertEquals(List.of("message", "it:news:sports", "goal"), message(s2));
        }
    }

    @Test
    void pubsubUnsubscribeStopsDeliveryAndDisconnectCleansUp() throws Exception {
        try (RespClient p = conn()) {
            try (RespClient s = subscriber()) {
                s.list("SUBSCRIBE", "it:ps:gone");
                assertEquals(1, p.integer("PUBLISH", "it:ps:gone", "1"));
                // The message may arrive before the unsubscribe confirmation.
                s.send("UNSUBSCRIBE", "it:ps:gone");
                List<String> r = RespClient.asList(s.read());
                if (r.get(0).equals("message")) {
                    r = RespClient.asList(s.read());
                }
                assertEquals("unsubscribe", r.get(0));
                assertEquals(0, p.integer("PUBLISH", "it:ps:gone", "2"));
                s.list("SUBSCRIBE", "it:ps:gone");
            } // connection dropped while subscribed
            Thread.sleep(200);
            assertEquals(0, p.integer("PUBLISH", "it:ps:gone", "3"), "dropped client must be unregistered");
        }
    }

    @Test
    void pubsubIntrospectionAndSubscribedModeRules() {
        try (RespClient s = subscriber(); RespClient c = conn()) {
            s.sendRaw("SUBSCRIBE it:ps:mode\r\n".getBytes(StandardCharsets.US_ASCII));
            assertEquals("*3\r\n$9\r\nsubscribe\r\n$10\r\nit:ps:mode\r\n:1\r\n", RespClient.utf8(s.readRaw()));

            List<String> ns = c.list("PUBSUB", "NUMSUB", "it:ps:mode");
            assertEquals(2, ns.size());
            assertEquals(List.of("it:ps:mode"), c.list("PUBSUB", "CHANNELS", "it:ps:m*"));

            // Regular commands are rejected while subscribed; PING is allowed.
            s.sendRaw("GET foo\r\n".getBytes(StandardCharsets.US_ASCII));
            assertTrue(RespClient.utf8(s.readRaw()).contains("Can't execute 'get'"));
            s.sendRaw("PING\r\n".getBytes(StandardCharsets.US_ASCII));
            assertEquals("*2\r\n$4\r\npong\r\n$0\r\n\r\n", RespClient.utf8(s.readRaw()));
        }
    }

    // -----------------------------------------------------------------------
    // sharding
    // -----------------------------------------------------------------------

    @Test
    void multiKeyCommandsSpanShards() {
        try (RespClient c = conn()) {
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                keys.add("it:mk:" + i);
            }
            List<Object> mset = new ArrayList<>(List.of("MSET"));
            for (String k : keys) {
                mset.add(k);
                mset.add("v-" + k);
            }
            c.ok(mset.toArray());

            // MGET re-assembles replies from every shard in request order.
            List<String> wanted = new ArrayList<>(keys);
            Collections.reverse(wanted);
            wanted.add(5, "it:mk:missing");
            List<Object> mget = new ArrayList<>(List.of("MGET"));
            mget.addAll(wanted);
            List<String> expect = new ArrayList<>();
            for (String k : wanted) {
                expect.add(k.equals("it:mk:missing") ? null : "v-" + k);
            }
            assertEquals(expect, c.list(mget.toArray()));

            List<Object> exists = new ArrayList<>(List.of("EXISTS"));
            exists.addAll(keys);
            exists.add(keys.get(0));
            exists.add("it:mk:missing");
            assertEquals(33, c.integer(exists.toArray()), "duplicates count twice, missing keys zero");
            assertEquals(32, c.list("KEYS", "it:mk:*").size());
            Frame.Array scan = (Frame.Array) RespClient.check(c.call("SCAN", 0, "MATCH", "it:mk:1*"));
            assertEquals("0", RespClient.asString(scan.items().get(0)));
            assertEquals(11, RespClient.asList(scan.items().get(1)).size()); // 1, 10..19

            List<Object> del = new ArrayList<>(List.of("DEL"));
            del.addAll(keys.subList(0, 20));
            assertEquals(20, c.integer(del.toArray()));
            List<Object> left = new ArrayList<>(List.of("EXISTS"));
            left.addAll(keys);
            assertEquals(12, c.integer(left.toArray()));
        }
    }

    private static List<Long> sortedLongs(List<String> l) {
        return l.stream().map(Long::parseLong).sorted().toList();
    }

    @Test
    void setAlgebraAcrossShards() {
        try (RespClient c = conn()) {
            // Different keys almost surely land on different shards.
            c.integer("SADD", "it:sa:a", 1, 2, 3, 4);
            c.integer("SADD", "it:sa:b", 3, 4, 5);
            c.integer("SADD", "it:sa:c", 4, 9);
            assertEquals(List.of(4L), sortedLongs(c.list("SINTER", "it:sa:a", "it:sa:b", "it:sa:c")));
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 9L),
                    sortedLongs(c.list("SUNION", "it:sa:a", "it:sa:b", "it:sa:c")));
            assertEquals(List.of(1L, 2L), sortedLongs(c.list("SDIFF", "it:sa:a", "it:sa:b", "it:sa:nope")));
            c.ok("SET", "it:sa:str", "x");
            RespError err = assertThrows(RespError.class, () -> c.list("SINTER", "it:sa:a", "it:sa:str"));
            assertTrue(err.getMessage().contains("WRONGTYPE"));
        }
    }

    @Test
    void pipelineMixingShardsKeepsPerKeyOrder() {
        try (RespClient c = conn()) {
            List<Object[]> pipe = new ArrayList<>();
            for (int round = 0; round < 20; round++) {
                for (int k = 0; k < 16; k++) {
                    pipe.add(new Object[] {"INCR", "it:po:" + k});
                    if (round == 10) {
                        pipe.add(new Object[] {"MGET", "it:po:" + k, "it:po:" + ((k + 1) % 16)});
                    }
                }
            }
            List<Long> ints = new ArrayList<>();
            for (Frame f : c.pipeline(pipe)) {
                if (f instanceof Frame.Int i) {
                    ints.add(i.value());
                }
            }
            List<Long> expected = new ArrayList<>();
            for (long r = 1; r <= 20; r++) {
                for (int k = 0; k < 16; k++) {
                    expected.add(r);
                }
            }
            assertEquals(expected, ints);
        }
    }

    @Test
    void globalCommandsAggregateEveryShard() throws IOException {
        try (Server srv = Server.startLocal(Config.defaults().withShards(SHARDS));
                RespClient c = new RespClient(srv.port())) {
            for (int i = 0; i < 200; i++) {
                c.ok("SET", "k" + i, i);
            }
            c.ok("SET", "ttl", "v", "EX", 100);
            assertEquals(201, c.integer("DBSIZE"));

            assertEquals("4", c.info("forgekv_shards"));
            String info = c.str("INFO", "shards");
            List<Long> perShard = new ArrayList<>();
            for (String line : info.split("\r\n")) {
                int at = line.indexOf("keys=");
                if (at >= 0) {
                    perShard.add(Long.parseLong(line.substring(at + 5).split(",")[0]));
                }
            }
            assertEquals(4, perShard.size());
            assertEquals(201, perShard.stream().mapToLong(Long::longValue).sum());
            assertTrue(perShard.stream().allMatch(k -> k > 20), "keys spread over every shard: " + perShard);
            String ks = c.str("INFO", "keyspace");
            assertTrue(ks.contains("db0:keys=201,expires=1"), ks);

            c.ok("FLUSHALL");
            assertEquals(0, c.integer("DBSIZE"));
        }
    }

    @Test
    void hashTagsMakeMultiKeyCommandsShardLocal() {
        try (RespClient c = conn()) {
            c.ok("MSET", "{ev:7}:stock", "10", "{ev:7}:sold", "0");
            assertEquals(List.of("10", "0"), c.list("MGET", "{ev:7}:stock", "{ev:7}:sold"));
            assertEquals(KeySlot.shardOf(Bytes.of("{ev:7}:stock"), SHARDS),
                    KeySlot.shardOf(Bytes.of("{ev:7}:sold"), SHARDS));
        }
    }

    // -----------------------------------------------------------------------
    // AOF persistence
    // -----------------------------------------------------------------------

    private static Config aofConfig(Path base, int shards, FsyncPolicy fsync, IoBackend backend) {
        return Config.defaults().withShards(shards).withAof(new AofConfig(base, fsync, backend));
    }

    private static void checkRestored(RespClient c) {
        assertEquals("alice", c.str("GET", "user"));
        long ttl = c.integer("TTL", "user");
        assertTrue(ttl >= 90 && ttl <= 100, "TTL carried over as an absolute deadline, got " + ttl);
        assertEquals("42", c.str("GET", "visits"));
        assertEquals(List.of("j2", "j3"), c.list("LRANGE", "queue", 0, -1));
        assertEquals("gold", c.hgetall("profile").get("tier"));
        assertEquals(3, c.integer("SCARD", "tags"));
        assertEquals("3", c.str("GET", "stock"));
        assertNull(c.str("GET", "short-lived"), "expired key stays dead");
        assertNull(c.str("GET", "flushed"), "FLUSHALL replays");
        for (int i = 0; i < 50; i++) {
            assertEquals(Integer.toString(i), c.str("GET", "bulk:" + i));
        }
    }

    @Test
    void dataSurvivesRestartAndReshardWithAof() throws Exception {
        Path base = tmpDir.resolve("appendonly.aof");

        // --- first life: 4 shards, a bit of everything ---
        try (Server srv = Server.startLocal(aofConfig(base, 4, FsyncPolicy.ALWAYS, IoBackend.STD));
                RespClient c = new RespClient(srv.port())) {
            c.ok("SET", "flushed", "x");
            c.ok("FLUSHALL");
            c.ok("SET", "user", "alice", "EX", 100);
            c.integer("INCRBY", "visits", 41);
            c.integer("INCRBY", "visits", 1);
            c.integer("RPUSH", "queue", "j1", "j2", "j3");
            c.str("LPOP", "queue");
            c.integer("HSET", "profile", "name", "Alice", "tier", "gold");
            c.integer("SADD", "tags", "a", "b", "c");
            c.ok("SET", "stock", 5);
            c.integer("DECRIF", "stock", 0, 2);
            assertNull(c.integer("DECRIF", "stock", 0, 9));
            List<Object> mset = new ArrayList<>(List.of("MSET"));
            for (int i = 0; i < 50; i++) {
                mset.add("bulk:" + i);
                mset.add(i);
            }
            c.ok(mset.toArray());
            c.ok("SET", "short-lived", "x", "PX", 50);
            Thread.sleep(400); // active expiry removes it and logs the DEL
            assertEquals("1", c.info("aof_enabled"));
            checkRestored(c);
        } // graceful shutdown: every shard fsyncs

        // --- second life: same shard count replays the same files ---
        try (Server srv = Server.startLocal(aofConfig(base, 4, FsyncPolicy.ALWAYS, IoBackend.STD));
                RespClient c = new RespClient(srv.port())) {
            checkRestored(c);
        }

        // --- third life: re-sharded to 3, then compacted ---
        try (Server srv = Server.startLocal(aofConfig(base, 3, FsyncPolicy.ALWAYS, IoBackend.STD));
                RespClient c = new RespClient(srv.port())) {
            checkRestored(c);
            String manifest = Files.readString(base.resolveSibling("appendonly.manifest"));
            assertTrue(manifest.contains("shards 3") && manifest.contains("generation 2"), manifest);
            for (int i = 0; i < 500; i++) {
                c.integer("INCR", "churn");
            }
            long before = Long.parseLong(c.info("aof_current_size"));
            assertTrue(c.str("BGREWRITEAOF").contains("rewriting"));
            long after = Long.parseLong(c.info("aof_current_size"));
            assertTrue(after < before, "rewrite should shrink the logs (" + before + " -> " + after + ")");
        }

        // --- fourth life: back to a single shard ---
        try (Server srv = Server.startLocal(aofConfig(base, 1, FsyncPolicy.ALWAYS, IoBackend.STD));
                RespClient c = new RespClient(srv.port())) {
            checkRestored(c);
            assertEquals("500", c.str("GET", "churn"));
            List<String> dir;
            try (Stream<Path> s = Files.list(tmpDir)) {
                dir = s.map(p -> p.getFileName().toString()).sorted().toList();
            }
            assertEquals(List.of("appendonly.g3.s0.aof", "appendonly.manifest"), dir, "old generations cleaned up");
        }
    }

    @Test
    void ioUringBackendEndToEnd() throws Exception {
        if (!Ring.available()) {
            System.err.println("io_uring unavailable; skipping");
            return;
        }
        Path base = tmpDir.resolve("appendonly.aof");
        try (Server srv = Server.startLocal(aofConfig(base, 2, FsyncPolicy.ALWAYS, IoBackend.URING));
                RespClient c = new RespClient(srv.port())) {
            assertEquals("io_uring", c.info("aof_io_backend"));
            // Many concurrent writers: replies only come back once fsynced.
            int port = srv.port();
            List<Thread> writers = new ArrayList<>();
            List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
            for (int w = 0; w < 8; w++) {
                int wi = w;
                Thread t = new Thread(() -> {
                    try (RespClient wc = new RespClient(port)) {
                        List<Object[]> pipe = new ArrayList<>();
                        for (int i = 0; i < 250; i++) {
                            pipe.add(new Object[] {"RPUSH", "log:" + wi, i});
                        }
                        wc.pipeline(pipe).forEach(RespClient::check);
                        for (int i = 0; i < 100; i++) {
                            wc.integer("INCR", "total");
                        }
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                });
                writers.add(t);
                t.start();
            }
            for (Thread t : writers) {
                t.join();
            }
            assertTrue(failures.isEmpty(), failures.toString());
            assertEquals("800", c.str("GET", "total"));
        }
        try (Server srv = Server.startLocal(aofConfig(base, 2, FsyncPolicy.EVERYSEC, IoBackend.URING));
                RespClient c = new RespClient(srv.port())) {
            assertEquals("800", c.str("GET", "total"));
            List<String> expected = new ArrayList<>();
            for (int i = 0; i < 250; i++) {
                expected.add(Integer.toString(i));
            }
            for (int w = 0; w < 8; w++) {
                assertEquals(expected, c.list("LRANGE", "log:" + w, 0, -1), "log:" + w + " intact and in order");
            }
        }
    }

    @Test
    void bgrewriteaofWithoutAofIsAnError() {
        try (RespClient c = conn()) {
            assertTrue(c.call("BGREWRITEAOF") instanceof Frame.Error);
        }
    }
}
