package forgekv.server;

import forgekv.resp.Frame;
import forgekv.storage.Clock;
import forgekv.util.Bytes;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;

/** Server-wide commands that need every shard: {@code INFO}, {@code BGREWRITEAOF}. */
final class Admin {
    static final String VERSION = "0.1.0";

    private Admin() {}

    /** Collect {@link ShardStats} from every shard; {@code cb} runs on {@code shard}'s thread. */
    static void gatherStats(Shard shard, Consumer<ShardStats[]> cb) {
        Router router = shard.router;
        ShardStats[] out = new ShardStats[router.n];
        java.util.Arrays.fill(out, ShardStats.EMPTY);
        out[shard.idx] = shard.stats();
        int[] waiting = {0};
        for (int s = 0; s < router.n; s++) {
            if (s == shard.idx) {
                continue;
            }
            int target = s;
            boolean sent = router.send(s, new ShardMsg.Run(other -> {
                ShardStats st = other.stats();
                shard.post(new ShardMsg.Run(me -> {
                    out[target] = st;
                    if (--waiting[0] == 0) {
                        cb.accept(out);
                    }
                }));
            }));
            if (sent) {
                waiting[0]++;
            }
        }
        if (waiting[0] == 0) {
            cb.accept(out);
        }
    }

    private static long sum(ShardStats[] stats, ToLongFunction<ShardStats> f) {
        long total = 0;
        for (ShardStats s : stats) {
            total += f.applyAsLong(s);
        }
        return total;
    }

    /** {@code INFO [section]} */
    static void info(Shard shard, Bytes[] args, Consumer<Frame> cb) {
        String want = args.length > 1 ? args[1].toString().toLowerCase(java.util.Locale.ROOT) : null;
        gatherStats(shard, stats -> cb.accept(Frame.bulk(render(shard.router, want, stats))));
    }

    private static boolean show(String want, String name) {
        return want == null || want.equals("all") || want.equals("everything") || want.equals("default")
                || want.equals(name);
    }

    private static String render(Router router, String want, ShardStats[] stats) {
        StringBuilder s = new StringBuilder();
        if (show(want, "server")) {
            s.append("# Server\r\nredis_version:7.2.0\r\nforgekv_version:").append(VERSION)
                    .append("\r\nredis_mode:standalone\r\narch_bits:64\r\nmultiplexing_api:java-nio\r\nforgekv_shards:")
                    .append(router.n)
                    .append("\r\nprocess_id:").append(ProcessHandle.current().pid())
                    .append("\r\ntcp_port:").append(router.port)
                    .append("\r\nuptime_in_seconds:").append(Math.max(0, Clock.nowMs() - router.startMs) / 1000)
                    .append("\r\n\r\n");
        }
        if (show(want, "clients")) {
            s.append("# Clients\r\nconnected_clients:").append(sum(stats, ShardStats::connectedClients))
                    .append("\r\n\r\n");
        }
        if (show(want, "stats")) {
            s.append("# Stats\r\ntotal_connections_received:").append(sum(stats, ShardStats::totalConnections))
                    .append("\r\ntotal_commands_processed:").append(sum(stats, ShardStats::commands))
                    .append("\r\nexpired_keys:").append(sum(stats, x -> x.db().expiredKeys()))
                    .append("\r\nkeyspace_hits:").append(sum(stats, x -> x.db().keyspaceHits()))
                    .append("\r\nkeyspace_misses:").append(sum(stats, x -> x.db().keyspaceMisses()))
                    .append("\r\n\r\n");
        }
        if (show(want, "replication")) {
            s.append("# Replication\r\n").append(router.repl.info()).append("\r\n");
        }
        if (show(want, "persistence")) {
            s.append("# Persistence\r\naof_enabled:").append(router.aofEnabled ? 1 : 0)
                    .append("\r\naof_current_size:").append(sum(stats, ShardStats::aofSize))
                    .append("\r\naof_fsyncs:").append(sum(stats, ShardStats::aofFsyncs))
                    .append("\r\naof_io_backend:").append(stats[0].aofBackend())
                    .append("\r\n\r\n");
        }
        if (show(want, "keyspace")) {
            s.append("# Keyspace\r\n");
            long keys = sum(stats, ShardStats::keys);
            if (keys > 0) {
                s.append("db0:keys=").append(keys).append(",expires=").append(sum(stats, ShardStats::expires))
                        .append(",avg_ttl=0\r\n");
            }
            s.append("\r\n");
        }
        if (show(want, "shards")) {
            s.append("# Shards\r\n");
            for (int i = 0; i < stats.length; i++) {
                ShardStats st = stats[i];
                s.append("shard").append(i).append(":keys=").append(st.keys()).append(",expires=")
                        .append(st.expires()).append(",commands=").append(st.commands()).append(",clients=")
                        .append(st.connectedClients()).append("\r\n");
            }
        }
        return s.toString();
    }

    /** {@code BGREWRITEAOF}: every shard compacts its own log. */
    static void bgrewriteaof(Shard shard, Consumer<Frame> cb) {
        Router router = shard.router;
        if (!router.aofEnabled) {
            cb.accept(Frame.error("ERR append only file is not enabled"));
            return;
        }
        String[] firstError = {null};
        int[] waiting = {0};
        Runnable reply = () -> cb.accept(firstError[0] == null
                ? Frame.simple("Background append only file rewriting started")
                : Frame.error("ERR " + firstError[0]));
        for (int s = 0; s < router.n; s++) {
            if (s == shard.idx) {
                continue;
            }
            boolean sent = router.send(s, new ShardMsg.Run(other -> {
                String err = other.rewriteAof();
                shard.post(new ShardMsg.Run(me -> {
                    if (firstError[0] == null && err != null) {
                        firstError[0] = err;
                    }
                    if (--waiting[0] == 0) {
                        reply.run();
                    }
                }));
            }));
            if (sent) {
                waiting[0]++;
            }
        }
        String own = shard.rewriteAof();
        if (own != null && firstError[0] == null) {
            firstError[0] = own;
        }
        if (waiting[0] == 0) {
            reply.run();
        }
    }
}
