package forgekv.server.cmd;

import static forgekv.server.cmd.Commands.parseInt;
import static forgekv.server.cmd.Commands.syntaxError;
import static forgekv.server.cmd.Commands.wrongArgs;

import forgekv.resp.Frame;
import forgekv.server.KeySlot;
import forgekv.server.cmd.Commands.Ctx;
import forgekv.util.Bytes;
import java.time.Instant;

/** Connection and server-management commands. */
final class AdminCmds {
    private AdminCmds() {}

    static Frame ping(Ctx ctx, Bytes[] args) throws CmdError {
        return switch (args.length) {
            case 1 -> Frame.simple("PONG");
            case 2 -> new Frame.Bulk(args[1]);
            default -> throw new CmdError(wrongArgs(args));
        };
    }

    static Frame echo(Ctx ctx, Bytes[] args) {
        return new Frame.Bulk(args[1]);
    }

    /**
     * {@code COMMAND} / {@code COMMAND DOCS}: redis-cli calls this on connect;
     * an empty reply is accepted and simply disables hints.
     */
    static Frame command(Ctx ctx, Bytes[] args) {
        return Frame.EMPTY_ARRAY;
    }

    static Frame select(Ctx ctx, Bytes[] args) throws CmdError {
        if (parseInt(args[1]) == 0) {
            return Frame.OK;
        }
        throw new CmdError(Frame.error("ERR DB index is out of range"));
    }

    static Frame dbsize(Ctx ctx, Bytes[] args) {
        return Frame.integer(ctx.db.size());
    }

    static Frame flushall(Ctx ctx, Bytes[] args) {
        ctx.db.clear();
        return Frame.OK;
    }

    /**
     * {@code FLUSHSHARD idx n [idx n ...]} (internal): delete every key that
     * belongs to shard {@code idx} of {@code n} under <em>all</em> the given layouts.
     *
     * <p>Persistence and replication log a {@code FLUSHALL} as
     * {@code FLUSHSHARD <shard> <n>} so that it only clears the keys its own
     * shard owns. That keeps per-shard logs independent: they can be replayed
     * in any order, into any number of shards, and interleaved on a
     * replication link without one shard's flush erasing another shard's
     * newer writes.
     */
    static Frame flushshard(Ctx ctx, Bytes[] args) throws CmdError {
        if ((args.length - 1) % 2 != 0) {
            throw new CmdError(wrongArgs(args));
        }
        int pairs = (args.length - 1) / 2;
        int[] idx = new int[pairs];
        int[] n = new int[pairs];
        for (int p = 0; p < pairs; p++) {
            long i = parseInt(args[1 + 2 * p]);
            long c = parseInt(args[2 + 2 * p]);
            if (c < 1 || i < 0 || i >= c || c > Integer.MAX_VALUE) {
                throw new CmdError(Frame.error("ERR invalid shard spec"));
            }
            idx[p] = (int) i;
            n[p] = (int) c;
        }
        ctx.db.removeWhere(k -> {
            for (int p = 0; p < pairs; p++) {
                if (KeySlot.shardOf(k, n[p]) != idx[p]) {
                    return false;
                }
            }
            return true;
        });
        return Frame.OK;
    }

    static Frame time(Ctx ctx, Bytes[] args) {
        long micros = Instant.now().getNano() / 1000;
        return Frame.array(Frame.bulk(Long.toString(ctx.now / 1000)), Frame.bulk(Long.toString(micros)));
    }

    /**
     * {@code CONFIG GET <pattern>} returns no entries; {@code CONFIG SET} is
     * accepted as a no-op. (redis-benchmark probes {@code CONFIG GET
     * save/appendonly} at startup.)
     */
    static Frame config(Ctx ctx, Bytes[] args) throws CmdError {
        return switch (args[1].toUpperAscii()) {
            case "GET" -> Frame.EMPTY_ARRAY;
            case "SET", "RESETSTAT", "REWRITE" -> Frame.OK;
            default -> throw new CmdError(syntaxError());
        };
    }

    /** {@code CLIENT SETNAME/SETINFO/...}: accepted so client libraries don't fail on connect. */
    static Frame client(Ctx ctx, Bytes[] args) {
        return switch (args[1].toUpperAscii()) {
            case "ID" -> Frame.integer(1);
            case "GETNAME" -> Frame.NULL;
            default -> Frame.OK;
        };
    }
}
