package forgekv.server.cmd;

import static forgekv.server.cmd.Commands.bulkOrNull;
import static forgekv.server.cmd.Commands.getString;
import static forgekv.server.cmd.Commands.invalidExpire;
import static forgekv.server.cmd.Commands.notInteger;
import static forgekv.server.cmd.Commands.overflow;
import static forgekv.server.cmd.Commands.parseInt;
import static forgekv.server.cmd.Commands.syntaxError;
import static forgekv.server.cmd.Commands.wrongArgs;

import forgekv.resp.Frame;
import forgekv.server.cmd.Commands.Ctx;
import forgekv.storage.Entry;
import forgekv.storage.Value;
import forgekv.util.Bytes;
import java.util.ArrayList;
import java.util.List;

/** String commands. */
final class StringCmds {
    private StringCmds() {}

    static Frame get(Ctx ctx, Bytes[] args) throws CmdError {
        return bulkOrNull(getString(ctx, args[1]));
    }

    /** {@code SET key value [NX|XX] [GET] [EX s|PX ms|EXAT ts|PXAT ts|KEEPTTL]} */
    static Frame set(Ctx ctx, Bytes[] args) throws CmdError {
        Bytes key = args[1];
        boolean nx = false, xx = false, get = false, keepttl = false;
        long expire = Entry.NO_EXPIRY;
        boolean hasExpire = false;

        for (int i = 3; i < args.length; i++) {
            String opt = args[i].toUpperAscii();
            switch (opt) {
                case "NX" -> nx = true;
                case "XX" -> xx = true;
                case "GET" -> get = true;
                case "KEEPTTL" -> keepttl = true;
                case "EX", "PX", "EXAT", "PXAT" -> {
                    if (hasExpire) {
                        throw new CmdError(syntaxError());
                    }
                    i++;
                    if (i >= args.length) {
                        throw new CmdError(syntaxError());
                    }
                    long n = parseInt(args[i]);
                    if (n <= 0) {
                        throw new CmdError(invalidExpire(args));
                    }
                    try {
                        expire = switch (opt) {
                            case "EX" -> Math.addExact(Math.multiplyExact(n, 1000L), ctx.now);
                            case "PX" -> Math.addExact(n, ctx.now);
                            case "EXAT" -> Math.multiplyExact(n, 1000L);
                            default -> n;
                        };
                    } catch (ArithmeticException e) {
                        throw new CmdError(invalidExpire(args));
                    }
                    hasExpire = true;
                }
                default -> throw new CmdError(syntaxError());
            }
        }
        if ((nx && xx) || (keepttl && hasExpire)) {
            throw new CmdError(syntaxError());
        }

        Bytes old = get ? getString(ctx, key) : null;
        boolean exists = ctx.db.contains(key, ctx.now);
        if ((nx && exists) || (xx && !exists)) {
            return get ? bulkOrNull(old) : Frame.NULL;
        }

        Value value = new Value.Str(args[2]);
        if (keepttl) {
            ctx.db.setKeepTtl(key, value, ctx.now);
        } else {
            ctx.db.set(key, value, expire);
        }
        return get ? bulkOrNull(old) : Frame.OK;
    }

    static Frame setnx(Ctx ctx, Bytes[] args) {
        if (ctx.db.contains(args[1], ctx.now)) {
            return Frame.integer(0);
        }
        ctx.db.set(args[1], new Value.Str(args[2]), Entry.NO_EXPIRY);
        return Frame.integer(1);
    }

    static Frame getdel(Ctx ctx, Bytes[] args) throws CmdError {
        Bytes v = getString(ctx, args[1]);
        if (v != null) {
            ctx.db.remove(args[1]);
        }
        return bulkOrNull(v);
    }

    static Frame mget(Ctx ctx, Bytes[] args) {
        // Non-string values yield nil rather than an error (Redis semantics).
        List<Frame> out = new ArrayList<>(args.length - 1);
        for (int i = 1; i < args.length; i++) {
            Bytes v;
            try {
                v = getString(ctx, args[i]);
            } catch (CmdError e) {
                v = null;
            }
            out.add(bulkOrNull(v));
        }
        return Frame.array(out);
    }

    static Frame mset(Ctx ctx, Bytes[] args) throws CmdError {
        if ((args.length - 1) % 2 != 0) {
            throw new CmdError(wrongArgs(args));
        }
        for (int i = 1; i < args.length; i += 2) {
            ctx.db.set(args[i], new Value.Str(args[i + 1]), Entry.NO_EXPIRY);
        }
        return Frame.OK;
    }

    /** Shared implementation of INCR/DECR/INCRBY/DECRBY. Preserves the TTL. */
    private static Frame incrBy(Ctx ctx, Bytes key, long delta) throws CmdError {
        Bytes cur = getString(ctx, key);
        long current;
        if (cur == null) {
            current = 0;
        } else {
            try {
                current = parseInt(cur);
            } catch (CmdError e) {
                throw new CmdError(notInteger());
            }
        }
        long next;
        try {
            next = Math.addExact(current, delta);
        } catch (ArithmeticException e) {
            throw new CmdError(overflow());
        }
        ctx.db.setKeepTtl(key, new Value.Str(Bytes.ofLong(next)), ctx.now);
        return Frame.integer(next);
    }

    static Frame incr(Ctx ctx, Bytes[] args) throws CmdError {
        return incrBy(ctx, args[1], 1);
    }

    static Frame decr(Ctx ctx, Bytes[] args) throws CmdError {
        return incrBy(ctx, args[1], -1);
    }

    static Frame incrby(Ctx ctx, Bytes[] args) throws CmdError {
        long d = parseInt(args[2]);
        return incrBy(ctx, args[1], d);
    }

    static Frame decrby(Ctx ctx, Bytes[] args) throws CmdError {
        long d = parseInt(args[2]);
        if (d == Long.MIN_VALUE) {
            throw new CmdError(overflow());
        }
        return incrBy(ctx, args[1], -d);
    }

    static Frame append(Ctx ctx, Bytes[] args) throws CmdError {
        Bytes old = getString(ctx, args[1]);
        Bytes next = old == null ? args[2] : old.concat(args[2]);
        ctx.db.setKeepTtl(args[1], new Value.Str(next), ctx.now);
        return Frame.integer(next.length());
    }

    static Frame strlen(Ctx ctx, Bytes[] args) throws CmdError {
        Bytes v = getString(ctx, args[1]);
        return Frame.integer(v == null ? 0 : v.length());
    }
}
