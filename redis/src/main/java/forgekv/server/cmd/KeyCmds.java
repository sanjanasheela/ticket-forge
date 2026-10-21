package forgekv.server.cmd;

import static forgekv.server.cmd.Commands.invalidExpire;
import static forgekv.server.cmd.Commands.parseInt;
import static forgekv.server.cmd.Commands.syntaxError;

import forgekv.resp.Frame;
import forgekv.server.Glob;
import forgekv.server.cmd.Commands.Ctx;
import forgekv.storage.Db;
import forgekv.storage.Entry;
import forgekv.util.Bytes;
import java.util.ArrayList;
import java.util.List;

/** Generic keyspace and TTL commands. */
final class KeyCmds {
    private KeyCmds() {}

    static Frame del(Ctx ctx, Bytes[] args) {
        long n = 0;
        for (int i = 1; i < args.length; i++) {
            // `contains` applies lazy expiry, so logically-expired keys don't count.
            if (ctx.db.contains(args[i], ctx.now)) {
                ctx.db.remove(args[i]);
                n++;
            }
        }
        return Frame.integer(n);
    }

    static Frame exists(Ctx ctx, Bytes[] args) {
        long n = 0;
        for (int i = 1; i < args.length; i++) {
            if (ctx.db.contains(args[i], ctx.now)) {
                n++;
            }
        }
        return Frame.integer(n);
    }

    static Frame type(Ctx ctx, Bytes[] args) {
        Entry e = ctx.db.get(args[1], ctx.now);
        return Frame.simple(e == null ? "none" : e.value().typeName());
    }

    /**
     * EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT.
     * A deadline in the past deletes the key immediately (Redis semantics).
     */
    private static Frame expireGeneric(Ctx ctx, Bytes[] args, long unitMs, boolean absolute) throws CmdError {
        Bytes key = args[1];
        long n = parseInt(args[2]);
        long at;
        try {
            long ms = Math.multiplyExact(n, unitMs);
            at = absolute ? ms : Math.addExact(ms, ctx.now);
        } catch (ArithmeticException e) {
            throw new CmdError(invalidExpire(args));
        }
        if (!ctx.db.contains(key, ctx.now)) {
            return Frame.integer(0);
        }
        if (at <= ctx.now) {
            ctx.db.remove(key);
        } else {
            ctx.db.setExpiry(key, at, ctx.now);
        }
        return Frame.integer(1);
    }

    static Frame expire(Ctx ctx, Bytes[] args) throws CmdError {
        return expireGeneric(ctx, args, 1000, false);
    }

    static Frame pexpire(Ctx ctx, Bytes[] args) throws CmdError {
        return expireGeneric(ctx, args, 1, false);
    }

    static Frame expireat(Ctx ctx, Bytes[] args) throws CmdError {
        return expireGeneric(ctx, args, 1000, true);
    }

    static Frame pexpireat(Ctx ctx, Bytes[] args) throws CmdError {
        return expireGeneric(ctx, args, 1, true);
    }

    private static Frame ttlGeneric(Ctx ctx, Bytes[] args, boolean seconds) {
        long ms = ctx.db.ttlMs(args[1], ctx.now);
        if (ms == Db.TTL_MISSING || ms == Db.TTL_NONE) {
            return Frame.integer(ms);
        }
        return Frame.integer(seconds ? (ms + 500) / 1000 : ms);
    }

    static Frame ttl(Ctx ctx, Bytes[] args) {
        return ttlGeneric(ctx, args, true);
    }

    static Frame pttl(Ctx ctx, Bytes[] args) {
        return ttlGeneric(ctx, args, false);
    }

    static Frame persist(Ctx ctx, Bytes[] args) {
        boolean hasTtl = ctx.db.ttlMs(args[1], ctx.now) >= 0;
        if (hasTtl) {
            ctx.db.setExpiry(args[1], Entry.NO_EXPIRY, ctx.now);
        }
        return Frame.integer(hasTtl ? 1 : 0);
    }

    private static List<Frame> matchingKeys(Ctx ctx, Bytes pattern, Bytes typeFilter) {
        boolean matchAll = pattern.length() == 1 && pattern.at(0) == '*';
        List<Frame> out = new ArrayList<>();
        ctx.db.forEachLive(ctx.now, (k, e) -> {
            if ((matchAll || Glob.match(pattern.array(), k.array()))
                    && (typeFilter == null || typeFilter.equalsIgnoreCase(e.value().typeName()))) {
                out.add(new Frame.Bulk(k));
            }
        });
        return out;
    }

    static Frame keys(Ctx ctx, Bytes[] args) {
        return Frame.array(matchingKeys(ctx, args[1], null));
    }

    private static final Bytes STAR = Bytes.of("*");

    /**
     * {@code SCAN cursor [MATCH pattern] [COUNT n] [TYPE t]}.
     * Replies with everything in one batch and cursor {@code 0}; a non-zero
     * cursor (from a client resuming an old scan) yields an empty final batch.
     */
    static Frame scan(Ctx ctx, Bytes[] args) throws CmdError {
        long cursor;
        try {
            cursor = parseInt(args[1]);
        } catch (CmdError e) {
            throw new CmdError(Frame.error("ERR invalid cursor"));
        }
        Bytes pattern = STAR;
        Bytes typeFilter = null;
        for (int i = 2; i < args.length; i += 2) {
            String opt = args[i].toUpperAscii();
            if (i + 1 >= args.length) {
                throw new CmdError(syntaxError());
            }
            Bytes val = args[i + 1];
            switch (opt) {
                case "MATCH" -> pattern = val;
                case "COUNT" -> {
                    if (parseInt(val) < 1) {
                        throw new CmdError(syntaxError());
                    }
                }
                case "TYPE" -> typeFilter = val;
                default -> throw new CmdError(syntaxError());
            }
        }
        List<Frame> batch = cursor == 0 ? matchingKeys(ctx, pattern, typeFilter) : List.of();
        return Frame.array(Frame.bulk("0"), Frame.array(batch));
    }
}
