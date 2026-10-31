package forgekv.server.cmd;

import static forgekv.server.cmd.Commands.getString;
import static forgekv.server.cmd.Commands.overflow;
import static forgekv.server.cmd.Commands.parseInt;
import static forgekv.server.cmd.Commands.syntaxError;
import static forgekv.server.cmd.Commands.wrongArgs;

import forgekv.resp.Frame;
import forgekv.server.cmd.Commands.Ctx;
import forgekv.storage.Value;
import forgekv.util.Bytes;

/**
 * Flash-sale primitives: ForgeKV extensions for the TicketForge reservation engine.
 *
 * <p>Each command is atomic because the shard's event loop executes one
 * command at a time. With stock Redis you'd need a Lua script for these;
 * here they're single round-trips.
 *
 * <ul>
 *   <li>{@code CAS key expected new}: 1 if swapped, 0 otherwise. Keeps the TTL.
 *   <li>{@code CAD key expected}: 1 if deleted, 0 otherwise. Safe lock
 *       release: only the holder of {@code token} can unlock.
 *   <li>{@code DECRIF key floor [n]}: decrement by {@code n} (default 1) only
 *       if the result stays &gt;= {@code floor}; returns the new value or nil.
 *       Guarantees zero overselling.
 * </ul>
 */
final class FlashCmds {
    private FlashCmds() {}

    static Frame cas(Ctx ctx, Bytes[] args) throws CmdError {
        Bytes cur = getString(ctx, args[1]);
        if (cur != null && cur.equals(args[2])) {
            ctx.db.setKeepTtl(args[1], new Value.Str(args[3]), ctx.now);
            return Frame.integer(1);
        }
        return Frame.integer(0);
    }

    static Frame cad(Ctx ctx, Bytes[] args) throws CmdError {
        Bytes cur = getString(ctx, args[1]);
        if (cur != null && cur.equals(args[2])) {
            ctx.db.remove(args[1]);
            return Frame.integer(1);
        }
        return Frame.integer(0);
    }

    static Frame decrif(Ctx ctx, Bytes[] args) throws CmdError {
        if (args.length > 4) {
            throw new CmdError(wrongArgs(args));
        }
        long floor = parseInt(args[2]);
        long amount = args.length == 4 ? parseInt(args[3]) : 1;
        if (amount <= 0) {
            throw new CmdError(syntaxError());
        }
        Bytes cur = getString(ctx, args[1]);
        long current = cur == null ? 0 : parseInt(cur);
        long next;
        try {
            next = Math.subtractExact(current, amount);
        } catch (ArithmeticException e) {
            throw new CmdError(overflow());
        }
        if (next < floor) {
            return Frame.NULL;
        }
        ctx.db.setKeepTtl(args[1], new Value.Str(Bytes.ofLong(next)), ctx.now);
        return Frame.integer(next);
    }
}
