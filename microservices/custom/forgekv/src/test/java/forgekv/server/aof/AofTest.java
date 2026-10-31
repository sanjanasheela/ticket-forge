package forgekv.server.aof;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import forgekv.resp.Frame;
import forgekv.server.KeySlot;
import forgekv.server.cmd.Commands;
import forgekv.server.uring.Ring;
import forgekv.storage.Clock;
import forgekv.storage.Db;
import forgekv.storage.Value;
import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AofTest {
    @TempDir
    Path dir;
    private int n;

    /** {@code <fresh dir>/appendonly.aof}. */
    private Path tmp(String tag) throws IOException {
        Path d = dir.resolve(tag + "-" + (n++));
        Files.createDirectories(d);
        return d.resolve("appendonly.aof");
    }

    private static List<IoBackend> backends() {
        return Ring.available() ? List.of(IoBackend.STD, IoBackend.URING) : List.of(IoBackend.STD);
    }

    private static Bytes b(String s) {
        return Bytes.of(s);
    }

    /** A single-shard "server": execute + feed + append, like a shard does. */
    private static final class Live implements AutoCloseable {
        final Db db = new Db();
        final Aof aof;
        final ByteBuf fx = new ByteBuf();
        final int shard;
        final int shards;

        Live(Path path, IoBackend backend) throws IOException {
            this(path, backend, 0, 1);
        }

        Live(Path path, IoBackend backend, int shard, int shards) throws IOException {
            db.setTrackExpired(true);
            aof = Aof.open(path, FsyncPolicy.NO, backend);
            this.shard = shard;
            this.shards = shards;
        }

        Frame run(String... parts) throws IOException {
            Bytes[] args = new Bytes[parts.length];
            for (int i = 0; i < parts.length; i++) {
                args[i] = Bytes.of(parts[i]);
            }
            Frame r = Commands.execute(db, Clock.nowMs(), args);
            AofLog.feed(fx, db, shard, shards, args, r);
            commit();
            return r;
        }

        void logExpired() throws IOException {
            for (Bytes key : db.drainExpired()) {
                AofLog.encodeCommand(fx, AofLog.DEL, key);
            }
            commit();
        }

        void commit() throws IOException {
            aof.appendRaw(fx);
            fx.clear();
            aof.flush();
            aof.groupSync();
        }

        void sync() throws IOException {
            aof.syncNow();
        }

        @Override
        public void close() {
            aof.close();
        }
    }

    private record Snap(Bytes key, Value value, long expiresAt) {}

    private static List<Snap> snap(Db db) {
        List<Snap> v = new ArrayList<>();
        db.forEachLive(0, (k, e) -> v.add(new Snap(k, e.value(), e.expiresAt())));
        v.sort((x, y) -> x.key.compareTo(y.key));
        return v;
    }

    /** Assert two databases hold identical live data and deadlines. */
    private static void assertSameData(Db a, Db b) {
        assertEquals(snap(a), snap(b));
    }

    @Test
    void replayReproducesEveryCommandType() throws IOException {
        for (IoBackend backend : backends()) {
            Path path = tmp("replay");
            try (Live s = new Live(path, backend)) {
                String[][] cmds = {
                    {"SET", "a", "1", "EX", "1000"},
                    {"SET", "b", "x", "NX"},
                    {"SET", "b", "ignored", "NX"}, // fails, must not be logged
                    {"SET", "c", "old", "PX", "99999"},
                    {"SET", "c", "new", "KEEPTTL"},
                    {"INCR", "n"},
                    {"INCRBY", "n", "41"},
                    {"APPEND", "b", "yz"},
                    {"MSET", "m1", "1", "m2", "2"},
                    {"RPUSH", "l", "1", "2", "3", "4"},
                    {"LPUSH", "l", "0"},
                    {"LPOP", "l"},
                    {"RPOP", "l", "2"},
                    {"HSET", "h", "f", "v", "g", "w"},
                    {"HINCRBY", "h", "cnt", "7"},
                    {"HDEL", "h", "g"},
                    {"SADD", "s", "a", "b", "c"},
                    {"SREM", "s", "a"},
                    {"EXPIRE", "b", "500"},
                    {"EXPIRE", "l", "700"},
                    {"PERSIST", "m1"},
                    {"SET", "stock", "3"},
                    {"DECRIF", "stock", "0", "2"},
                    {"DECRIF", "stock", "0", "2"}, // refused, must not be logged
                    {"SET", "lock", "tok"},
                    {"CAS", "lock", "tok", "tok2"},
                    {"CAD", "lock", "nope"}, // refused
                    {"SET", "gone", "x"},
                    {"GETDEL", "gone"},
                    {"SET", "del1", "x"},
                    {"DEL", "del1", "missing"},
                    {"SETNX", "nx", "v"},
                    {"SET", "past", "v"},
                    {"EXPIREAT", "past", "1"}, // past deadline deletes
                };
                for (String[] cmd : cmds) {
                    s.run(cmd);
                }
                s.sync();

                Db replayed = new Db();
                AofLog.LoadReport report = AofLog.load(path, replayed);
                assertEquals(0, report.errors(), backend.toString());
                assertSameData(s.db, replayed);
                // Spot-check semantics, not just equality.
                assertNull(replayed.peek(b("gone")));
                assertNull(replayed.peek(b("past")));
                assertEquals(new Value.Str(b("1")), replayed.peek(b("stock")).value());
                assertTrue(replayed.peek(b("c")).hasExpiry(), "KEEPTTL kept the deadline");
            }
        }
    }

    @Test
    void expiryIsReplayedEvenWhenItChangesLaterCommands() throws Exception {
        Path path = tmp("expiry");
        try (Live s = new Live(path, IoBackend.STD)) {
            s.run("SET", "k", "5", "PX", "20");
            Thread.sleep(50);
            // Lazily expires "k", then recreates it as 1. Replay must agree even
            // though it runs *after* the deadline on a frozen clock.
            assertEquals(Frame.integer(1), s.run("INCR", "k"));
            s.run("SET", "e", "v", "PX", "20");
            Thread.sleep(50);
            s.db.activeExpireCycle(Clock.nowMs());
            s.logExpired(); // what a shard's expiry task does after each cycle
            s.sync();

            Db replayed = new Db();
            AofLog.load(path, replayed);
            assertEquals(new Value.Str(b("1")), replayed.peek(b("k")).value());
            assertNull(replayed.peek(b("e")), "actively-expired key must stay deleted");
            assertSameData(s.db, replayed);
        }
    }

    @Test
    void tornTailIsTruncatedAndPrefixSurvives() throws IOException {
        Path path = tmp("torn");
        try (Live s = new Live(path, IoBackend.STD)) {
            s.run("SET", "keep", "me");
            s.run("RPUSH", "l", "a", "b");
            s.sync();
        }
        long goodLen = Files.size(path);
        // Simulate a crash in the middle of writing the next command.
        try (OutputStream o = Files.newOutputStream(path, StandardOpenOption.APPEND)) {
            o.write("*3\r\n$3\r\nSET\r\n$4\r\nhal".getBytes(StandardCharsets.US_ASCII));
        }

        Db db = new Db();
        AofLog.LoadReport report = AofLog.load(path, db);
        assertTrue(report.truncatedBytes() > 0);
        assertEquals(goodLen, Files.size(path), "file truncated to last good command");
        assertEquals(2, db.size());
        // A second load is clean.
        assertEquals(0, AofLog.load(path, new Db()).truncatedBytes());
    }

    @Test
    void corruptionInTheMiddleIsAHardError() throws IOException {
        Path path = tmp("corrupt");
        Files.write(path, "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$zz\r\nboom\r\n".getBytes(StandardCharsets.US_ASCII));
        AofLog.CorruptedException err =
                assertThrows(AofLog.CorruptedException.class, () -> AofLog.load(path, new Db()));
        assertTrue(err.getMessage().contains("corrupted"));
    }

    @Test
    void rewriteCompactsAndPreservesState() throws IOException {
        for (IoBackend backend : backends()) {
            Path path = tmp("rewrite");
            try (Live s = new Live(path, backend)) {
                for (int i = 0; i < 2_000; i++) {
                    s.run("SET", "hot", Integer.toString(i));
                    s.run("INCR", "counter");
                }
                s.run("SET", "ttl", "v", "EX", "3600");
                s.run("RPUSH", "l", "1", "2", "3");
                s.run("EXPIRE", "l", "3600");
                for (int i = 0; i < 600; i++) {
                    s.run("SADD", "bigset", "m" + i); // > REWRITE_CHUNK members
                }
                s.run("HSET", "h", "a", "1");
                s.sync();
                long before = Files.size(path);

                s.aof.rewrite(s.db, Clock.nowMs());
                long after = Files.size(path);
                assertTrue(after < before / 10, "log should shrink a lot: " + before + " -> " + after);
                assertEquals(after, s.aof.size());

                Db replayed = new Db();
                assertEquals(0, AofLog.load(path, replayed).errors());
                assertSameData(s.db, replayed);

                // Appending keeps working on the new file and replays on top of it.
                s.run("INCR", "counter");
                s.sync();
                Db replayed2 = new Db();
                AofLog.load(path, replayed2);
                assertSameData(s.db, replayed2);
                assertEquals(new Value.Str(b("2001")), replayed2.peek(b("counter")).value());
            }
        }
    }

    @Test
    void uringGroupCommitAndDurabilitySequencing() throws IOException {
        if (!Ring.available()) {
            return;
        }
        Path path = tmp("uring");
        try (Aof aof = Aof.open(path, FsyncPolicy.ALWAYS, IoBackend.URING)) {
            assertEquals("io_uring", aof.backendName());
            ByteBuf expected = new ByteBuf();
            long last = 0;
            for (int i = 0; i < 500; i++) {
                Bytes[] cmd = {b("SET"), b("k" + i), b("v" + i)};
                AofLog.encodeCommand(expected, cmd);
                aof.append(cmd);
                last = aof.flush();
                assertTrue(aof.durableSeq() <= last);
                if (i % 7 == 0) {
                    aof.groupSync(); // end of an event-loop pass
                }
                if (i % 50 == 0) {
                    // Reap whatever completed so far, as the event loop would.
                    aof.onCompletion();
                }
            }
            assertEquals(500, last);
            aof.syncNow();
            assertEquals(aof.lastSeq(), aof.durableSeq());
            assertArrayEquals(expected.toByteArray(), Files.readAllBytes(path), "byte-exact, in order, no holes");
            assertEquals(expected.readable(), aof.size());
        }
    }

    @Test
    void stdAlwaysIsDurableOnlyAfterGroupSync() throws IOException {
        try (Aof aof = Aof.open(tmp("group"), FsyncPolicy.ALWAYS, IoBackend.STD)) {
            aof.append(b("SET"), b("a"), b("1"));
            long s1 = aof.flush();
            aof.append(b("SET"), b("b"), b("2"));
            long s2 = aof.flush();
            assertEquals(1, s1);
            assertEquals(2, s2);
            assertTrue(aof.needsGroupSync());
            assertEquals(0, aof.durableSeq(), "written, not yet fsynced");
            aof.groupSync();
            assertEquals(2, aof.durableSeq(), "one fdatasync covers both");
            assertFalse(aof.needsGroupSync());
        }
        try (Aof no = Aof.open(tmp("group-no"), FsyncPolicy.NO, IoBackend.STD)) {
            no.append(b("PING"));
            assertEquals(no.flush(), no.durableSeq(), "no fsync to wait for");
        }
    }

    @Test
    void manifestRoundtripAndPaths() throws IOException {
        Path base = tmp("manifest");
        assertNull(AofLog.readManifest(base));
        AofLog.writeManifest(base, new AofLog.Layout(3, 4));
        assertEquals(new AofLog.Layout(3, 4), AofLog.readManifest(base));
        assertTrue(AofLog.shardPath(base, 3, 1).endsWith("appendonly.g3.s1.aof"));
        assertTrue(AofLog.manifestPath(base).endsWith("appendonly.manifest"));
        Files.writeString(AofLog.manifestPath(base), "garbage");
        assertThrows(IOException.class, () -> AofLog.readManifest(base));
    }

    @Test
    void perShardLogsReplayInAnyOrderAndRepartition() throws IOException {
        Path base = tmp("layout");
        AofLog.Recovered fresh = AofLog.recover(base, 4);
        assertEquals(new AofLog.Layout(1, 4), fresh.layout());
        assertTrue(fresh.dbs().stream().allMatch(Db::isEmpty));

        // Simulate 4 shards writing through their own logs.
        List<Live> shards = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            shards.add(new Live(fresh.files().get(i), IoBackend.STD, i, 4));
        }
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            keys.add("key:" + i);
        }
        for (String k : keys) {
            shards.get(KeySlot.shardOf(Bytes.of(k), 4)).run("SET", k, "old");
        }
        // FLUSHALL reaches every shard; each logs a FLUSHSHARD for its own keys.
        for (Live s : shards) {
            s.run("FLUSHALL");
        }
        // Newer writes after the flush, on every shard.
        for (int i = 0; i < keys.size(); i += 3) {
            String k = keys.get(i);
            Live owner = shards.get(KeySlot.shardOf(Bytes.of(k), 4));
            owner.run("SET", k, "new");
            owner.run("RPUSH", "{" + k + "}:list", "x");
        }
        int want = 0;
        for (Live s : shards) {
            s.sync();
            want += s.db.size();
            s.close();
        }

        // Replay the logs in reverse order into one keyspace: same result.
        Db combined = new Db();
        List<Path> reversed = new ArrayList<>(fresh.files());
        Collections.reverse(reversed);
        for (Path f : reversed) {
            AofLog.load(f, combined);
        }
        assertEquals(want, combined.size());
        for (int i = 0; i < keys.size(); i += 3) {
            assertEquals(new Value.Str(b("new")), combined.peek(b(keys.get(i))).value());
        }

        // Restart with 3 shards: re-partitioned into generation 2.
        AofLog.Recovered r3 = AofLog.recover(base, 3);
        assertEquals(new AofLog.Layout(2, 3), r3.layout());
        for (Path f : fresh.files()) {
            assertFalse(Files.exists(f), "old generation removed: " + f);
        }
        for (int i = 0; i < 3; i++) {
            int idx = i;
            r3.dbs().get(i).forEachLive(0, (k, e) -> assertEquals(idx, KeySlot.shardOf(k, 3), "keys land on their owner"));
        }
        assertEquals(combined.size(), r3.dbs().stream().mapToInt(Db::size).sum());

        // Same shard count again: files are reused as-is.
        AofLog.Recovered again = AofLog.recover(base, 3);
        assertEquals(r3.layout(), again.layout());
        assertEquals(r3.files(), again.files());
    }

    @Test
    void legacySingleFileIsImported() throws IOException {
        Path base = tmp("legacy");
        try (Live s = new Live(base, IoBackend.STD)) {
            s.run("SET", "a", "1");
            s.run("FLUSHALL");
            s.run("SET", "b", "2");
            s.sync();
        }
        AofLog.Recovered r = AofLog.recover(base, 2);
        assertEquals(2, r.layout().shards());
        assertFalse(Files.exists(base), "legacy file replaced by the sharded layout");
        assertEquals(1, r.dbs().stream().mapToInt(Db::size).sum());
        assertNotNull(r.dbs().get(KeySlot.shardOf(b("b"), 2)).peek(b("b")));
    }

    @Test
    void optionParsing() {
        assertEquals(FsyncPolicy.ALWAYS, FsyncPolicy.parse("always"));
        assertEquals(FsyncPolicy.EVERYSEC, FsyncPolicy.parse("EverySec"));
        assertThrows(IllegalArgumentException.class, () -> FsyncPolicy.parse("sometimes"));
        assertEquals(IoBackend.URING, IoBackend.parse("io_uring"));
        assertEquals(IoBackend.STD, IoBackend.parse("std"));
        assertThrows(IllegalArgumentException.class, () -> IoBackend.parse("epoll"));
    }
}
