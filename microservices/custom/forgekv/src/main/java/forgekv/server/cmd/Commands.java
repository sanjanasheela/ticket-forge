package forgekv.server.cmd;

import forgekv.resp.Frame;
import forgekv.storage.Db;
import forgekv.storage.Entry;
import forgekv.storage.Value;
import forgekv.util.Bytes;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Command table and dispatch.
 *
 * <p>Every handler has the same signature ({@link Handler}) and returns a
 * {@link Frame}; error replies are thrown as {@link CmdError} so they can be
 * propagated like Rust's {@code ?}.
 *
 * <p>Handlers only ever see a single shard's {@link Db}. The table also
 * describes <em>where</em> a command's keys are ({@link Keys}) and whether it
 * writes, which the router uses to split multi-key commands across shards and
 * to enforce read-only replicas.
 */
public final class Commands {
    private Commands() {}

    /** Execution context handed to every command. */
    public static final class Ctx {
        public final Db db;
        public final long now;

        Ctx(Db db, long now) {
            this.db = db;
            this.now = now;
        }
    }

    @FunctionalInterface
    public interface Handler {
        Frame run(Ctx ctx, Bytes[] args) throws CmdError;
    }

    /** Where a command's keys live, i.e. how it is routed between shards. */
    public enum Keys {
        /** No keys: runs on whichever shard received it. */
        NONE,
        /** One key at {@code args[1]}. */
        FIRST,
        /** Every argument after the name is a key. */
        ALL,
        /** {@code key value [key value ...]}. */
        PAIRS,
        /** Touches the whole keyspace: runs on every shard, results are combined. */
        GLOBAL,
    }

    /**
     * @param arity Redis arity: {@code n} = exactly n args (incl. name), {@code -n} = at least n
     */
    public record Spec(String name, int arity, boolean write, Keys keys, Handler handler) {}

    private static final boolean R = false;
    private static final boolean W = true;
    private static final Map<String, Spec> TABLE = new HashMap<>();

    private static void def(String name, int arity, boolean write, Keys keys, Handler h) {
        TABLE.put(name, new Spec(name, arity, write, keys, h));
    }

    static {
        // connection / server
        def("PING", -1, R, Keys.NONE, AdminCmds::ping);
        def("ECHO", 2, R, Keys.NONE, AdminCmds::echo);
        def("COMMAND", -1, R, Keys.NONE, AdminCmds::command);
        def("SELECT", 2, R, Keys.NONE, AdminCmds::select);
        def("CONFIG", -2, R, Keys.NONE, AdminCmds::config);
        def("CLIENT", -2, R, Keys.NONE, AdminCmds::client);
        def("TIME", 1, R, Keys.NONE, AdminCmds::time);
        def("DBSIZE", 1, R, Keys.GLOBAL, AdminCmds::dbsize);
        def("FLUSHALL", -1, W, Keys.GLOBAL, AdminCmds::flushall);
        def("FLUSHDB", -1, W, Keys.GLOBAL, AdminCmds::flushall);
        def("FLUSHSHARD", -3, W, Keys.GLOBAL, AdminCmds::flushshard);
        // strings
        def("GET", 2, R, Keys.FIRST, StringCmds::get);
        def("SET", -3, W, Keys.FIRST, StringCmds::set);
        def("SETNX", 3, W, Keys.FIRST, StringCmds::setnx);
        def("GETDEL", 2, W, Keys.FIRST, StringCmds::getdel);
        def("MGET", -2, R, Keys.ALL, StringCmds::mget);
        def("MSET", -3, W, Keys.PAIRS, StringCmds::mset);
        def("INCR", 2, W, Keys.FIRST, StringCmds::incr);
        def("DECR", 2, W, Keys.FIRST, StringCmds::decr);
        def("INCRBY", 3, W, Keys.FIRST, StringCmds::incrby);
        def("DECRBY", 3, W, Keys.FIRST, StringCmds::decrby);
        def("APPEND", 3, W, Keys.FIRST, StringCmds::append);
        def("STRLEN", 2, R, Keys.FIRST, StringCmds::strlen);
        // keyspace / ttl
        def("DEL", -2, W, Keys.ALL, KeyCmds::del);
        def("UNLINK", -2, W, Keys.ALL, KeyCmds::del);
        def("EXISTS", -2, R, Keys.ALL, KeyCmds::exists);
        def("TYPE", 2, R, Keys.FIRST, KeyCmds::type);
        def("EXPIRE", 3, W, Keys.FIRST, KeyCmds::expire);
        def("PEXPIRE", 3, W, Keys.FIRST, KeyCmds::pexpire);
        def("EXPIREAT", 3, W, Keys.FIRST, KeyCmds::expireat);
        def("PEXPIREAT", 3, W, Keys.FIRST, KeyCmds::pexpireat);
        def("TTL", 2, R, Keys.FIRST, KeyCmds::ttl);
        def("PTTL", 2, R, Keys.FIRST, KeyCmds::pttl);
        def("PERSIST", 2, W, Keys.FIRST, KeyCmds::persist);
        def("KEYS", 2, R, Keys.GLOBAL, KeyCmds::keys);
        def("SCAN", -2, R, Keys.GLOBAL, KeyCmds::scan);
        // lists
        def("LPUSH", -3, W, Keys.FIRST, CollectionCmds::lpush);
        def("RPUSH", -3, W, Keys.FIRST, CollectionCmds::rpush);
        def("LPOP", -2, W, Keys.FIRST, CollectionCmds::lpop);
        def("RPOP", -2, W, Keys.FIRST, CollectionCmds::rpop);
        def("LLEN", 2, R, Keys.FIRST, CollectionCmds::llen);
        def("LRANGE", 4, R, Keys.FIRST, CollectionCmds::lrange);
        def("LINDEX", 3, R, Keys.FIRST, CollectionCmds::lindex);
        // hashes
        def("HSET", -4, W, Keys.FIRST, CollectionCmds::hset);
        def("HMSET", -4, W, Keys.FIRST, CollectionCmds::hset);
        def("HGET", 3, R, Keys.FIRST, CollectionCmds::hget);
        def("HMGET", -3, R, Keys.FIRST, CollectionCmds::hmget);
        def("HDEL", -3, W, Keys.FIRST, CollectionCmds::hdel);
        def("HGETALL", 2, R, Keys.FIRST, CollectionCmds::hgetall);
        def("HKEYS", 2, R, Keys.FIRST, CollectionCmds::hkeys);
        def("HVALS", 2, R, Keys.FIRST, CollectionCmds::hvals);
        def("HLEN", 2, R, Keys.FIRST, CollectionCmds::hlen);
        def("HEXISTS", 3, R, Keys.FIRST, CollectionCmds::hexists);
        def("HINCRBY", 4, W, Keys.FIRST, CollectionCmds::hincrby);
        // sets
        def("SADD", -3, W, Keys.FIRST, CollectionCmds::sadd);
        def("SREM", -3, W, Keys.FIRST, CollectionCmds::srem);
        def("SMEMBERS", 2, R, Keys.FIRST, CollectionCmds::smembers);
        def("SISMEMBER", 3, R, Keys.FIRST, CollectionCmds::sismember);
        def("SCARD", 2, R, Keys.FIRST, CollectionCmds::scard);
        def("SINTER", -2, R, Keys.ALL, CollectionCmds::sinter);
        def("SUNION", -2, R, Keys.ALL, CollectionCmds::sunion);
        def("SDIFF", -2, R, Keys.ALL, CollectionCmds::sdiff);
        // flash-sale primitives (ForgeKV extensions)
        def("CAS", 4, W, Keys.FIRST, FlashCmds::cas);
        def("CAD", 3, W, Keys.FIRST, FlashCmds::cad);
        def("DECRIF", -3, W, Keys.FIRST, FlashCmds::decrif);
    }

    /** Look up a command by its upper-cased name, or {@code null}. */
    public static Spec lookup(String upperName) {
        return TABLE.get(upperName);
    }

    /**
     * The upper-cased command name. Names longer than any command become
     * empty (and match nothing).
     */
    public static String name(Bytes raw) {
        return raw.length() <= 32 ? raw.toUpperAscii() : "";
    }

    public static boolean arityOk(int argc, int arity) {
        return arity >= 0 ? argc == arity : argc >= -arity;
    }

    /**
     * Run one command against a single shard's {@code db} at logical time
     * {@code now}. {@code args} is non-empty. Pure with respect to persistence
     * and replication: AOF replay and replicas call this with {@code now = 0}
     * (a frozen clock).
     */
    public static Frame execute(Db db, long now, Bytes[] args) {
        return executeWith(db, now, args, null);
    }

    /**
     * {@link #execute} with the command's {@link Spec} when the caller already
     * looked it up (arity already checked).
     */
    public static Frame executeWith(Db db, long now, Bytes[] args, Spec spec) {
        if (spec == null) {
            spec = lookup(name(args[0]));
            if (spec == null) {
                return unknownCommand(args);
            }
            if (!arityOk(args.length, spec.arity())) {
                return wrongArgs(args);
            }
        }
        try {
            return spec.handler().run(new Ctx(db, now), args);
        } catch (CmdError e) {
            return e.frame;
        }
    }

    // -----------------------------------------------------------------------
    // Shared helpers & canonical Redis error replies
    // -----------------------------------------------------------------------

    public static String cmdName(Bytes[] args) {
        return args[0].toString().toLowerCase(Locale.ROOT);
    }

    public static Frame wrongArgs(Bytes[] args) {
        return Frame.error("ERR wrong number of arguments for '" + cmdName(args) + "' command");
    }

    public static Frame unknownCommand(Bytes[] args) {
        StringBuilder rest = new StringBuilder();
        for (int i = 1; i < args.length && i <= 16; i++) {
            rest.append('\'').append(args[i]).append("' ");
        }
        return Frame.error("ERR unknown command '" + args[0] + "', with args beginning with: " + rest);
    }

    public static Frame syntaxError() {
        return Frame.error("ERR syntax error");
    }

    public static Frame notInteger() {
        return Frame.error("ERR value is not an integer or out of range");
    }

    public static Frame overflow() {
        return Frame.error("ERR increment or decrement would overflow");
    }

    public static Frame wrongType() {
        return Frame.error("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    public static Frame invalidExpire(Bytes[] args) {
        return Frame.error("ERR invalid expire time in '" + cmdName(args) + "' command");
    }

    /** Strict signed 64-bit integer parsing (rejects whitespace and {@code +}). */
    public static long parseInt(Bytes b) throws CmdError {
        byte[] a = b.array();
        if (a.length == 0 || a.length > 20 || a[0] == '+') {
            throw new CmdError(notInteger());
        }
        int i = 0;
        boolean neg = a[0] == '-';
        if (neg) {
            i = 1;
            if (a.length == 1) {
                throw new CmdError(notInteger());
            }
        }
        long v = 0;
        for (; i < a.length; i++) {
            int d = a[i] - '0';
            if (d < 0 || d > 9 || v < (Long.MIN_VALUE + d) / 10) {
                throw new CmdError(notInteger());
            }
            v = v * 10 - d;
        }
        if (!neg) {
            if (v == Long.MIN_VALUE) {
                throw new CmdError(notInteger());
            }
            v = -v;
        }
        return v;
    }

    /** Fetch a string value or {@code null}; {@code WRONGTYPE} for other types. */
    static Bytes getString(Ctx ctx, Bytes key) throws CmdError {
        Entry e = ctx.db.get(key, ctx.now);
        if (e == null) {
            return null;
        }
        if (e.value() instanceof Value.Str s) {
            return s.value();
        }
        throw new CmdError(wrongType());
    }

    static Frame bulkOrNull(Bytes v) {
        return v == null ? Frame.NULL : new Frame.Bulk(v);
    }
}
