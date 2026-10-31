package forgekv.server;

import forgekv.storage.DbStats;

/** Per-shard numbers aggregated by {@code INFO}. */
public record ShardStats(
        long keys,
        long expires,
        DbStats db,
        long commands,
        long connectedClients,
        long totalConnections,
        long aofSize,
        long aofFsyncs,
        String aofBackend) {
    public static final ShardStats EMPTY = new ShardStats(0, 0, DbStats.ZERO, 0, 0, 0, 0, 0, "none");
}
