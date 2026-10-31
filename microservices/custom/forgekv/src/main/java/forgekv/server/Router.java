package forgekv.server;

import forgekv.server.repl.Replication;
import forgekv.storage.Clock;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Process-wide state shared by all shard threads: the shard mailboxes, the
 * Pub/Sub registry and the replication hub. Everything else (keyspace, AOF,
 * counters) is owned by exactly one shard.
 */
public final class Router {
    /** Number of shards. */
    public final int n;
    private final Shard[] shards;
    public final PubSub pubsub = new PubSub();
    public final Replication repl;
    public final int port;
    public final long startMs = Clock.nowMs();
    public final boolean aofEnabled;
    private final AtomicLong clientIds = new AtomicLong();

    Router(int n, int port, boolean aofEnabled, int backlogSize) {
        this.n = n;
        this.shards = new Shard[n];
        this.port = port;
        this.aofEnabled = aofEnabled;
        this.repl = new Replication(n, backlogSize);
    }

    void install(int idx, Shard shard) {
        shards[idx] = shard;
    }

    public Shard shard(int idx) {
        return shards[idx];
    }

    /** Post a message to a shard. {@code false} if that shard has stopped. */
    public boolean send(int shard, ShardMsg msg) {
        return shards[shard].post(msg);
    }

    public void broadcast(Supplier<ShardMsg> msg) {
        for (int s = 0; s < n; s++) {
            send(s, msg.get());
        }
    }

    public long nextClientId() {
        return clientIds.incrementAndGet();
    }
}
