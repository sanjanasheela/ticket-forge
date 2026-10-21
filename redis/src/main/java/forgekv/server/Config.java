package forgekv.server;

import forgekv.server.aof.AofConfig;

/**
 * Startup configuration.
 *
 * @param aof {@code null} = purely in-memory
 * @param shards number of shards (threads); 0 = one per available CPU
 * @param replicaofHost start as a replica of this leader ({@code null} = leader)
 * @param replBacklogSize bytes of replication stream kept for partial resyncs
 */
public record Config(AofConfig aof, int shards, String replicaofHost, int replicaofPort, int replBacklogSize) {
    public static Config defaults() {
        return new Config(null, 1, null, 0, 1 << 20);
    }

    public Config withShards(int n) {
        return new Config(aof, n, replicaofHost, replicaofPort, replBacklogSize);
    }

    public Config withAof(AofConfig a) {
        return new Config(a, shards, replicaofHost, replicaofPort, replBacklogSize);
    }

    public Config withReplicaof(String host, int port) {
        return new Config(aof, shards, host, port, replBacklogSize);
    }

    public Config withReplBacklogSize(int bytes) {
        return new Config(aof, shards, replicaofHost, replicaofPort, bytes);
    }

    /** The effective shard count. */
    public int shardCount() {
        return shards == 0 ? Math.max(1, Runtime.getRuntime().availableProcessors()) : shards;
    }
}
