package forgekv.server.cmd;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import forgekv.resp.Frame;
import forgekv.server.KeySlot;
import forgekv.storage.Clock;
import forgekv.storage.Db;
import forgekv.util.Bytes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class CommandsTest {
    private static Frame run(Db db, String... parts) {
        Bytes[] args = new Bytes[parts.length];
        for (int i = 0; i < parts.length; i++) {
            args[i] = Bytes.of(parts[i]);
        }
        return Commands.execute(db, Clock.nowMs(), args);
    }

    private static Frame bulk(String s) {
        return Frame.bulk(s);
    }

    private static long intOf(Frame f) {
        return ((Frame.Int) f).value();
    }

    private static List<String> sorted(Frame f) {
        if (!(f instanceof Frame.Array a)) {
            throw new AssertionError("expected array, got " + f);
        }
        List<String> v = new ArrayList<>();
        for (Frame i : a.items()) {
            if (!(i instanceof Frame.Bulk b)) {
                throw new AssertionError("unexpected " + i);
            }
            v.add(b.value().toString());
        }
        Collections.sort(v);
        return v;
    }

    @Test
    void arityAndUnknown() {
        Db s = new Db();
        assertEquals(Frame.error("ERR wrong number of arguments for 'get' command"), run(s, "GET"));
        assertTrue(run(s, "NOPE", "x") instanceof Frame.Error e && e.message().startsWith("ERR unknown command 'NOPE'"));
        assertEquals(Frame.simple("PONG"), run(s, "ping"));
    }

    @Test
    void setOptions() {
        Db s = new Db();
        assertEquals(Frame.OK, run(s, "SET", "k", "v", "NX"));
        assertEquals(Frame.NULL, run(s, "SET", "k", "v2", "NX"));
        assertEquals(bulk("v"), run(s, "SET", "k", "v3", "XX", "GET"));
        assertEquals(Frame.NULL, run(s, "SET", "missing", "v", "XX"));
        assertEquals(Commands.syntaxError(), run(s, "SET", "k", "v", "NX", "XX"));
        assertEquals(Frame.error("ERR invalid expire time in 'set' command"), run(s, "SET", "k", "v", "EX", "0"));
        assertEquals(Commands.syntaxError(), run(s, "SET", "k", "v", "EX"));
        assertEquals(Frame.OK, run(s, "SET", "k", "v", "PX", "5000"));
        long pttl = intOf(run(s, "PTTL", "k"));
        assertTrue(pttl > 4000 && pttl <= 5000);
        assertEquals(Frame.OK, run(s, "SET", "k", "v", "KEEPTTL"));
        assertTrue(intOf(run(s, "PTTL", "k")) > 4000);
        assertEquals(Frame.OK, run(s, "SET", "k", "v"));
        assertEquals(Frame.integer(-1), run(s, "TTL", "k"));
    }

    @Test
    void counters() {
        Db s = new Db();
        assertEquals(Frame.integer(1), run(s, "INCR", "n"));
        assertEquals(Frame.integer(42), run(s, "INCRBY", "n", "41"));
        assertEquals(Frame.integer(40), run(s, "DECRBY", "n", "2"));
        assertEquals(Frame.OK, run(s, "SET", "s", "abc"));
        assertEquals(Commands.notInteger(), run(s, "INCR", "s"));
        assertEquals(Frame.OK, run(s, "SET", "big", Long.toString(Long.MAX_VALUE)));
        assertEquals(Commands.overflow(), run(s, "INCR", "big"));
        assertEquals(Commands.notInteger(), run(s, "INCRBY", "n", "+1"));
    }

    @Test
    void expiryCommands() {
        Db s = new Db();
        assertEquals(Frame.integer(-2), run(s, "TTL", "k"));
        run(s, "SET", "k", "v");
        assertEquals(Frame.integer(1), run(s, "EXPIRE", "k", "100"));
        assertEquals(Frame.integer(100), run(s, "TTL", "k"));
        assertEquals(Frame.integer(1), run(s, "PERSIST", "k"));
        assertEquals(Frame.integer(0), run(s, "PERSIST", "k"));
        assertEquals(Frame.integer(1), run(s, "EXPIRE", "k", "-1"));
        assertEquals(Frame.integer(0), run(s, "EXISTS", "k"));
        assertEquals(Frame.integer(0), run(s, "EXPIRE", "k", "10"));
    }

    @Test
    void flashSalePrimitives() {
        Db s = new Db();
        run(s, "SET", "lock", "token-a");
        assertEquals(Frame.integer(0), run(s, "CAS", "lock", "token-b", "x"));
        assertEquals(Frame.integer(1), run(s, "CAS", "lock", "token-a", "token-c"));
        assertEquals(Frame.integer(0), run(s, "CAD", "lock", "token-a"));
        assertEquals(Frame.integer(1), run(s, "CAD", "lock", "token-c"));
        assertEquals(Frame.integer(0), run(s, "EXISTS", "lock"));

        run(s, "SET", "stock", "2");
        assertEquals(Frame.integer(1), run(s, "DECRIF", "stock", "0"));
        assertEquals(Frame.integer(0), run(s, "DECRIF", "stock", "0"));
        assertEquals(Frame.NULL, run(s, "DECRIF", "stock", "0"));
        assertEquals(bulk("0"), run(s, "GET", "stock"));
        run(s, "SET", "stock", "10");
        assertEquals(Frame.integer(6), run(s, "DECRIF", "stock", "0", "4"));
        assertEquals(Frame.NULL, run(s, "DECRIF", "stock", "0", "7"));
        assertEquals(Commands.syntaxError(), run(s, "DECRIF", "stock", "0", "0"));
    }

    @Test
    void lists() {
        Db s = new Db();
        assertEquals(Frame.integer(3), run(s, "RPUSH", "l", "a", "b", "c"));
        assertEquals(Frame.integer(5), run(s, "LPUSH", "l", "x", "y"));
        // LPUSH x y => y x a b c
        assertEquals(Frame.array(bulk("y"), bulk("x"), bulk("a"), bulk("b"), bulk("c")),
                run(s, "LRANGE", "l", "0", "-1"));
        assertEquals(bulk("c"), run(s, "LINDEX", "l", "-1"));
        assertEquals(Frame.NULL, run(s, "LINDEX", "l", "99"));
        assertEquals(bulk("y"), run(s, "LPOP", "l"));
        assertEquals(Frame.array(bulk("c"), bulk("b")), run(s, "RPOP", "l", "2"));
        assertEquals(Frame.integer(2), run(s, "LLEN", "l"));
        assertEquals(Frame.array(bulk("x"), bulk("a")), run(s, "LPOP", "l", "10"));
        // Emptied list is deleted.
        assertEquals(Frame.integer(0), run(s, "EXISTS", "l"));
        assertEquals(Frame.NULL, run(s, "LPOP", "l"));
        assertEquals(Frame.NULL_ARRAY, run(s, "LPOP", "l", "3"));
        assertEquals(Frame.error("ERR value is out of range, must be positive"), run(s, "LPOP", "l", "-1"));
    }

    @Test
    void listsKeepTtl() {
        Db s = new Db();
        run(s, "RPUSH", "l", "a");
        run(s, "EXPIRE", "l", "100");
        run(s, "RPUSH", "l", "b");
        assertEquals(Frame.integer(100), run(s, "TTL", "l"));
    }

    @Test
    void hashes() {
        Db s = new Db();
        assertEquals(Frame.integer(2), run(s, "HSET", "h", "a", "1", "b", "2"));
        assertEquals(Frame.integer(1), run(s, "HSET", "h", "a", "9", "c", "3"));
        assertEquals(bulk("9"), run(s, "HGET", "h", "a"));
        assertEquals(Frame.NULL, run(s, "HGET", "h", "zz"));
        assertEquals(Frame.integer(3), run(s, "HLEN", "h"));
        assertEquals(Frame.integer(1), run(s, "HEXISTS", "h", "b"));
        assertEquals(Frame.integer(5), run(s, "HINCRBY", "h", "n", "5"));
        assertEquals(Frame.integer(10), run(s, "HINCRBY", "h", "a", "1"));
        assertEquals(Commands.notInteger(), run(s, "HINCRBY", "h", "b", "x"));
        assertEquals(List.of("a", "b", "c", "n"), sorted(run(s, "HKEYS", "h")));
        assertEquals(Frame.array(bulk("2"), Frame.NULL), run(s, "HMGET", "h", "b", "nope"));
        assertEquals(Frame.integer(4), run(s, "HDEL", "h", "a", "b", "c", "n", "zz"));
        assertEquals(Frame.integer(0), run(s, "EXISTS", "h"));
        assertEquals(Frame.error("ERR wrong number of arguments for 'hset' command"), run(s, "HSET", "h", "odd"));
    }

    @Test
    void sets() {
        Db s = new Db();
        assertEquals(Frame.integer(3), run(s, "SADD", "a", "1", "2", "3", "3"));
        assertEquals(Frame.integer(3), run(s, "SADD", "b", "2", "3", "4"));
        assertEquals(Frame.integer(3), run(s, "SCARD", "a"));
        assertEquals(Frame.integer(1), run(s, "SISMEMBER", "a", "2"));
        assertEquals(Frame.integer(0), run(s, "SISMEMBER", "a", "9"));
        assertEquals(List.of("2", "3"), sorted(run(s, "SINTER", "a", "b")));
        assertEquals(List.of("1", "2", "3", "4"), sorted(run(s, "SUNION", "a", "b")));
        assertEquals(List.of("1"), sorted(run(s, "SDIFF", "a", "b")));
        assertEquals(List.of(), sorted(run(s, "SINTER", "a", "missing")));
        assertEquals(Frame.integer(3), run(s, "SREM", "a", "1", "2", "3", "9"));
        assertEquals(Frame.integer(0), run(s, "EXISTS", "a"));
    }

    @Test
    void wrongtypeAndTypeCmd() {
        Db s = new Db();
        run(s, "SET", "str", "v");
        run(s, "RPUSH", "list", "v");
        run(s, "HSET", "hash", "f", "v");
        run(s, "SADD", "set", "v");
        assertEquals(Frame.simple("string"), run(s, "TYPE", "str"));
        assertEquals(Frame.simple("list"), run(s, "TYPE", "list"));
        assertEquals(Frame.simple("hash"), run(s, "TYPE", "hash"));
        assertEquals(Frame.simple("set"), run(s, "TYPE", "set"));
        assertEquals(Frame.simple("none"), run(s, "TYPE", "none"));
        String[][] cmds = {
            {"GET", "list"}, {"INCR", "hash"}, {"LPUSH", "str", "x"}, {"HGET", "set", "f"}, {"SADD", "list", "x"},
            {"LLEN", "hash"}, {"SINTER", "set", "str"},
        };
        for (String[] cmd : cmds) {
            assertEquals(Commands.wrongType(), run(s, cmd), String.join(" ", cmd));
        }
        // MGET yields nil for non-strings instead of failing.
        assertEquals(Frame.array(bulk("v"), Frame.NULL), run(s, "MGET", "str", "list"));
        // DEL works on every type.
        assertEquals(Frame.integer(4), run(s, "DEL", "str", "list", "hash", "set"));
    }

    @Test
    void keysAndScan() {
        Db s = new Db();
        for (String k : List.of("user:1", "user:2", "order:1")) {
            run(s, "SET", k, "v");
        }
        run(s, "SADD", "user:set", "m");
        assertEquals(List.of("user:1", "user:2", "user:set"), sorted(run(s, "KEYS", "user:*")));
        assertEquals(4, sorted(run(s, "KEYS", "*")).size());
        Frame.Array parts = (Frame.Array) run(s, "SCAN", "0", "MATCH", "user:?", "COUNT", "10", "TYPE", "string");
        assertEquals(bulk("0"), parts.items().get(0));
        assertEquals(List.of("user:1", "user:2"), sorted(parts.items().get(1)));
        assertEquals(Frame.error("ERR invalid cursor"), run(s, "SCAN", "abc"));
    }

    @Test
    void flushshardOnlyRemovesMatchingKeys() {
        Db s = new Db();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            keys.add("k" + i);
        }
        for (String k : keys) {
            run(s, "SET", k, "v");
        }
        // Delete shard 1 of 4.
        assertEquals(Frame.OK, run(s, "FLUSHSHARD", "1", "4"));
        for (String k : keys) {
            boolean alive = run(s, "EXISTS", k).equals(Frame.integer(1));
            assertEquals(KeySlot.shardOf(Bytes.of(k), 4) != 1, alive, k);
        }
        // Constraints are AND-ed: shard 0 of 2 *and* shard 0 of 4.
        run(s, "FLUSHSHARD", "0", "2", "0", "4");
        for (String k : keys) {
            boolean gone = KeySlot.shardOf(Bytes.of(k), 4) <= 1;
            assertEquals(gone, run(s, "EXISTS", k).equals(Frame.integer(0)), k);
        }
        assertEquals(Frame.error("ERR invalid shard spec"), run(s, "FLUSHSHARD", "4", "4"));
        assertEquals(Frame.error("ERR wrong number of arguments for 'flushshard' command"),
                run(s, "FLUSHSHARD", "0"));
    }

    @Test
    void rangeNormalisation() {
        assertArrayEquals(new long[] {0, 4}, CollectionCmds.normRange(5, 0, -1));
        assertArrayEquals(new long[] {3, 4}, CollectionCmds.normRange(5, -2, -1));
        assertArrayEquals(new long[] {1, 4}, CollectionCmds.normRange(5, 1, 100));
        assertArrayEquals(new long[] {0, 1}, CollectionCmds.normRange(5, -100, 1));
        assertNull(CollectionCmds.normRange(5, 3, 1));
        assertNull(CollectionCmds.normRange(5, 5, 9));
        assertNull(CollectionCmds.normRange(0, 0, -1));
    }
}
