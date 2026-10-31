package forgekv.server.cmd;

import static forgekv.server.cmd.Commands.overflow;
import static forgekv.server.cmd.Commands.parseInt;
import static forgekv.server.cmd.Commands.wrongArgs;
import static forgekv.server.cmd.Commands.wrongType;

import forgekv.resp.Frame;
import forgekv.server.cmd.Commands.Ctx;
import forgekv.storage.BytesDeque;
import forgekv.storage.Db;
import forgekv.storage.Entry;
import forgekv.storage.Value;
import forgekv.util.Bytes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * List, hash and set commands.
 *
 * <p>Invariant (same as Redis): a collection that becomes empty is deleted,
 * so {@code EXISTS}/{@code TYPE}/{@code DBSIZE} never see empty aggregates.
 * Mutating an existing collection never touches its TTL.
 */
final class CollectionCmds {
    private CollectionCmds() {}

    private static Frame bulkArray(Iterable<Bytes> items) {
        List<Frame> out = items instanceof Collection<?> c ? new ArrayList<>(c.size()) : new ArrayList<>();
        for (Bytes b : items) {
            out.add(new Frame.Bulk(b));
        }
        return Frame.array(out);
    }

    private static Frame outOfRange() {
        return Frame.error("ERR value is out of range, must be positive");
    }

    // -----------------------------------------------------------------------
    // typed accessors (WRONGTYPE aware)
    // -----------------------------------------------------------------------

    private static BytesDeque list(Db db, Bytes key, long now) throws CmdError {
        Value v = db.getMut(key, now);
        if (v == null) {
            return null;
        }
        if (v instanceof Value.ListV l) {
            return l.items();
        }
        throw new CmdError(wrongType());
    }

    private static HashMap<Bytes, Bytes> hash(Db db, Bytes key, long now) throws CmdError {
        Value v = db.getMut(key, now);
        if (v == null) {
            return null;
        }
        if (v instanceof Value.HashV h) {
            return h.map();
        }
        throw new CmdError(wrongType());
    }

    private static HashSet<Bytes> set(Db db, Bytes key, long now) throws CmdError {
        Value v = db.getMut(key, now);
        if (v == null) {
            return null;
        }
        if (v instanceof Value.SetV s) {
            return s.set();
        }
        throw new CmdError(wrongType());
    }

    /** Copy of a set's members; a missing key behaves as the empty set. */
    private static HashSet<Bytes> setSnapshot(Ctx ctx, Bytes key) throws CmdError {
        HashSet<Bytes> s = set(ctx.db, key, ctx.now);
        return s == null ? new HashSet<>() : new HashSet<>(s);
    }

    // -----------------------------------------------------------------------
    // lists
    // -----------------------------------------------------------------------

    private static Frame push(Ctx ctx, Bytes[] args, boolean front) throws CmdError {
        Bytes key = args[1];
        BytesDeque l = list(ctx.db, key, ctx.now);
        boolean created = l == null;
        if (created) {
            l = new BytesDeque();
        }
        for (int i = 2; i < args.length; i++) {
            if (front) {
                l.addFirst(args[i]);
            } else {
                l.addLast(args[i]);
            }
        }
        if (created) {
            ctx.db.set(key, new Value.ListV(l), Entry.NO_EXPIRY);
        }
        return Frame.integer(l.size());
    }

    static Frame lpush(Ctx ctx, Bytes[] args) throws CmdError {
        return push(ctx, args, true);
    }

    static Frame rpush(Ctx ctx, Bytes[] args) throws CmdError {
        return push(ctx, args, false);
    }

    private static Frame pop(Ctx ctx, Bytes[] args, boolean front) throws CmdError {
        if (args.length > 3) {
            throw new CmdError(wrongArgs(args));
        }
        Bytes key = args[1];
        long count = -1;
        if (args.length == 3) {
            long n = parseInt(args[2]);
            if (n < 0) {
                throw new CmdError(outOfRange());
            }
            count = n;
        }
        BytesDeque l = list(ctx.db, key, ctx.now);
        if (l == null) {
            return count >= 0 ? Frame.NULL_ARRAY : Frame.NULL;
        }
        Frame reply;
        if (count < 0) {
            Bytes b = front ? l.pollFirst() : l.pollLast();
            reply = b == null ? Frame.NULL : new Frame.Bulk(b);
        } else {
            List<Frame> out = new ArrayList<>();
            for (long i = 0; i < count; i++) {
                Bytes b = front ? l.pollFirst() : l.pollLast();
                if (b == null) {
                    break;
                }
                out.add(new Frame.Bulk(b));
            }
            reply = Frame.array(out);
        }
        if (l.isEmpty()) {
            ctx.db.remove(key);
        }
        return reply;
    }

    static Frame lpop(Ctx ctx, Bytes[] args) throws CmdError {
        return pop(ctx, args, true);
    }

    static Frame rpop(Ctx ctx, Bytes[] args) throws CmdError {
        return pop(ctx, args, false);
    }

    static Frame llen(Ctx ctx, Bytes[] args) throws CmdError {
        BytesDeque l = list(ctx.db, args[1], ctx.now);
        return Frame.integer(l == null ? 0 : l.size());
    }

    /**
     * Resolve Redis-style inclusive, possibly negative {@code [start, stop]}
     * indices. Returns {@code {s, e}} or {@code null} for an empty range.
     */
    static long[] normRange(long len, long start, long stop) {
        long s = start < 0 ? Math.max(len + start, 0) : start;
        long e = stop < 0 ? len + stop : Math.min(stop, len - 1);
        return (s <= e && s < len) ? new long[] {s, e} : null;
    }

    static Frame lrange(Ctx ctx, Bytes[] args) throws CmdError {
        long start = parseInt(args[2]);
        long stop = parseInt(args[3]);
        BytesDeque l = list(ctx.db, args[1], ctx.now);
        if (l == null) {
            return Frame.EMPTY_ARRAY;
        }
        long[] r = normRange(l.size(), start, stop);
        if (r == null) {
            return Frame.EMPTY_ARRAY;
        }
        List<Frame> out = new ArrayList<>((int) (r[1] - r[0] + 1));
        for (long i = r[0]; i <= r[1]; i++) {
            out.add(new Frame.Bulk(l.get((int) i)));
        }
        return Frame.array(out);
    }

    static Frame lindex(Ctx ctx, Bytes[] args) throws CmdError {
        long idx = parseInt(args[2]);
        BytesDeque l = list(ctx.db, args[1], ctx.now);
        if (l == null) {
            return Frame.NULL;
        }
        long i = idx < 0 ? l.size() + idx : idx;
        return (i >= 0 && i < l.size()) ? new Frame.Bulk(l.get((int) i)) : Frame.NULL;
    }

    // -----------------------------------------------------------------------
    // hashes
    // -----------------------------------------------------------------------

    static Frame hset(Ctx ctx, Bytes[] args) throws CmdError {
        if ((args.length - 2) % 2 != 0) {
            throw new CmdError(wrongArgs(args));
        }
        Bytes key = args[1];
        HashMap<Bytes, Bytes> h = hash(ctx.db, key, ctx.now);
        boolean created = h == null;
        if (created) {
            h = new HashMap<>();
        }
        long added = 0;
        for (int i = 2; i < args.length; i += 2) {
            if (h.put(args[i], args[i + 1]) == null) {
                added++;
            }
        }
        if (created) {
            ctx.db.set(key, new Value.HashV(h), Entry.NO_EXPIRY);
        }
        // HMSET (legacy alias) replies +OK, but HSET's integer is accepted by
        // every client for both, so we keep one code path for simplicity.
        return Frame.integer(added);
    }

    static Frame hget(Ctx ctx, Bytes[] args) throws CmdError {
        HashMap<Bytes, Bytes> h = hash(ctx.db, args[1], ctx.now);
        Bytes v = h == null ? null : h.get(args[2]);
        return v == null ? Frame.NULL : new Frame.Bulk(v);
    }

    static Frame hmget(Ctx ctx, Bytes[] args) throws CmdError {
        HashMap<Bytes, Bytes> h = hash(ctx.db, args[1], ctx.now);
        List<Frame> out = new ArrayList<>(args.length - 2);
        for (int i = 2; i < args.length; i++) {
            Bytes v = h == null ? null : h.get(args[i]);
            out.add(v == null ? Frame.NULL : new Frame.Bulk(v));
        }
        return Frame.array(out);
    }

    static Frame hdel(Ctx ctx, Bytes[] args) throws CmdError {
        HashMap<Bytes, Bytes> h = hash(ctx.db, args[1], ctx.now);
        if (h == null) {
            return Frame.integer(0);
        }
        long removed = 0;
        for (int i = 2; i < args.length; i++) {
            if (h.remove(args[i]) != null) {
                removed++;
            }
        }
        if (h.isEmpty()) {
            ctx.db.remove(args[1]);
        }
        return Frame.integer(removed);
    }

    static Frame hgetall(Ctx ctx, Bytes[] args) throws CmdError {
        HashMap<Bytes, Bytes> h = hash(ctx.db, args[1], ctx.now);
        if (h == null) {
            return Frame.EMPTY_ARRAY;
        }
        List<Frame> out = new ArrayList<>(h.size() * 2);
        for (Map.Entry<Bytes, Bytes> e : h.entrySet()) {
            out.add(new Frame.Bulk(e.getKey()));
            out.add(new Frame.Bulk(e.getValue()));
        }
        return Frame.array(out);
    }

    static Frame hkeys(Ctx ctx, Bytes[] args) throws CmdError {
        HashMap<Bytes, Bytes> h = hash(ctx.db, args[1], ctx.now);
        return h == null ? Frame.EMPTY_ARRAY : bulkArray(h.keySet());
    }

    static Frame hvals(Ctx ctx, Bytes[] args) throws CmdError {
        HashMap<Bytes, Bytes> h = hash(ctx.db, args[1], ctx.now);
        return h == null ? Frame.EMPTY_ARRAY : bulkArray(h.values());
    }

    static Frame hlen(Ctx ctx, Bytes[] args) throws CmdError {
        HashMap<Bytes, Bytes> h = hash(ctx.db, args[1], ctx.now);
        return Frame.integer(h == null ? 0 : h.size());
    }

    static Frame hexists(Ctx ctx, Bytes[] args) throws CmdError {
        HashMap<Bytes, Bytes> h = hash(ctx.db, args[1], ctx.now);
        return Frame.integer(h != null && h.containsKey(args[2]) ? 1 : 0);
    }

    static Frame hincrby(Ctx ctx, Bytes[] args) throws CmdError {
        long delta = parseInt(args[3]);
        Bytes key = args[1];
        HashMap<Bytes, Bytes> h = hash(ctx.db, key, ctx.now);
        boolean created = h == null;
        if (created) {
            h = new HashMap<>();
        }
        Bytes cur = h.get(args[2]);
        long current = 0;
        if (cur != null) {
            try {
                current = parseInt(cur);
            } catch (CmdError e) {
                throw new CmdError(Frame.error("ERR hash value is not an integer"));
            }
        }
        long next;
        try {
            next = Math.addExact(current, delta);
        } catch (ArithmeticException e) {
            throw new CmdError(overflow());
        }
        h.put(args[2], Bytes.ofLong(next));
        if (created) {
            ctx.db.set(key, new Value.HashV(h), Entry.NO_EXPIRY);
        }
        return Frame.integer(next);
    }

    // -----------------------------------------------------------------------
    // sets
    // -----------------------------------------------------------------------

    static Frame sadd(Ctx ctx, Bytes[] args) throws CmdError {
        Bytes key = args[1];
        HashSet<Bytes> s = set(ctx.db, key, ctx.now);
        boolean created = s == null;
        if (created) {
            s = new HashSet<>();
        }
        long added = 0;
        for (int i = 2; i < args.length; i++) {
            if (s.add(args[i])) {
                added++;
            }
        }
        if (created) {
            ctx.db.set(key, new Value.SetV(s), Entry.NO_EXPIRY);
        }
        return Frame.integer(added);
    }

    static Frame srem(Ctx ctx, Bytes[] args) throws CmdError {
        HashSet<Bytes> s = set(ctx.db, args[1], ctx.now);
        if (s == null) {
            return Frame.integer(0);
        }
        long removed = 0;
        for (int i = 2; i < args.length; i++) {
            if (s.remove(args[i])) {
                removed++;
            }
        }
        if (s.isEmpty()) {
            ctx.db.remove(args[1]);
        }
        return Frame.integer(removed);
    }

    static Frame smembers(Ctx ctx, Bytes[] args) throws CmdError {
        HashSet<Bytes> s = set(ctx.db, args[1], ctx.now);
        return s == null ? Frame.EMPTY_ARRAY : bulkArray(s);
    }

    static Frame sismember(Ctx ctx, Bytes[] args) throws CmdError {
        HashSet<Bytes> s = set(ctx.db, args[1], ctx.now);
        return Frame.integer(s != null && s.contains(args[2]) ? 1 : 0);
    }

    static Frame scard(Ctx ctx, Bytes[] args) throws CmdError {
        HashSet<Bytes> s = set(ctx.db, args[1], ctx.now);
        return Frame.integer(s == null ? 0 : s.size());
    }

    static Frame sinter(Ctx ctx, Bytes[] args) throws CmdError {
        HashSet<Bytes> acc = setSnapshot(ctx, args[1]);
        for (int i = 2; i < args.length; i++) {
            HashSet<Bytes> other = setSnapshot(ctx, args[i]);
            acc.retainAll(other);
        }
        return bulkArray(acc);
    }

    static Frame sunion(Ctx ctx, Bytes[] args) throws CmdError {
        HashSet<Bytes> acc = new HashSet<>();
        for (int i = 1; i < args.length; i++) {
            acc.addAll(setSnapshot(ctx, args[i]));
        }
        return bulkArray(acc);
    }

    static Frame sdiff(Ctx ctx, Bytes[] args) throws CmdError {
        HashSet<Bytes> acc = setSnapshot(ctx, args[1]);
        for (int i = 2; i < args.length; i++) {
            HashSet<Bytes> other = setSnapshot(ctx, args[i]);
            acc.removeAll(other);
        }
        return bulkArray(acc);
    }
}
