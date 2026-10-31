package forgekv.server;

import forgekv.server.aof.AofConfig;
import forgekv.server.aof.AofLog;
import forgekv.server.repl.Replication;
import forgekv.storage.Db;
import forgekv.util.Log;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * A running ForgeKV server.
 *
 * <p>Architecture: <b>thread-per-core, shared-nothing</b>. The keyspace is
 * split into {@code --shards} slices by key hash ({@link KeySlot}), and each
 * slice is owned by one OS thread running its own event loop
 * ({@link Shard}). Within a shard nothing is locked and every command is
 * atomic, just like Redis' single event loop. Shards cooperate only by
 * message passing ({@link Batch}, {@link Router}).
 *
 * <pre>
 *             acceptor thread ── round-robin hand-off of new sockets
 *                   │
 *      ┌────────────┼─────────────┐
 *   shard 0      shard 1   …   shard N-1      one thread + selector each
 *   Db, AOF      Db, AOF       Db, AOF        owned, never shared
 *      └──── mailboxes (batched cross-shard sub-commands) ───┘
 *                   │
 *   Router: Pub/Sub registry + replication hub (the only shared state)
 * </pre>
 *
 * <p>With {@code --shards 1} (the default) this is exactly the Redis model.
 *
 * <p>{@link #close()} shuts it down gracefully (final AOF fsync).
 */
public final class Server implements AutoCloseable {
    private final InetSocketAddress addr;
    private final Router router;
    private final List<Thread> shardThreads = new ArrayList<>();
    private ServerSocketChannel listener;
    private Thread acceptor;
    private boolean stopped;

    private Server(InetSocketAddress addr, Router router) {
        this.addr = addr;
        this.router = router;
    }

    /** Bind {@code host:port} (port 0 picks a free port) and start serving. */
    public static Server start(String host, int port, Config config) throws IOException {
        ServerSocketChannel l = ServerSocketChannel.open();
        try {
            l.bind(new InetSocketAddress(host, port), 1024);
        } catch (IOException e) {
            l.close();
            throw e;
        }
        return startOn(l, config);
    }

    /**
     * Start serving on an already-bound listener. Persisted data is loaded
     * before this returns, so the server is ready once it hands back.
     */
    public static Server startOn(ServerSocketChannel listener, Config config) throws IOException {
        int n = config.shardCount();
        InetSocketAddress addr = (InetSocketAddress) listener.getLocalAddress();
        AofConfig aofCfg = config.aof();

        List<Db> dbs;
        List<Path> files;
        try {
            if (aofCfg != null) {
                AofLog.Recovered r = AofLog.recover(aofCfg.path(), n);
                dbs = r.dbs();
                files = r.files();
            } else {
                dbs = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    dbs.add(new Db());
                }
                files = null;
            }
        } catch (IOException | RuntimeException e) {
            listener.close();
            throw e;
        }

        Router router = new Router(n, addr.getPort(), aofCfg != null, config.replBacklogSize());
        Server server = new Server(addr, router);
        List<Shard> shards = new ArrayList<>(n);
        try {
            for (int i = 0; i < n; i++) {
                Shard.ShardAof sa = files == null ? null
                        : new Shard.ShardAof(files.get(i), aofCfg.fsync(), aofCfg.backend());
                Shard shard = new Shard(i, router, dbs.get(i), sa);
                router.install(i, shard);
                shards.add(shard);
            }
        } catch (IOException e) {
            listener.close();
            throw e;
        }
        for (Shard shard : shards) {
            Thread t = new Thread(shard, "forgekv-shard-" + shard.idx);
            server.shardThreads.add(t);
            t.start();
        }
        IOException failure = null;
        for (Shard shard : shards) {
            try {
                shard.ready.get();
            } catch (ExecutionException e) {
                if (failure == null) {
                    failure = e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failure = new IOException("interrupted during startup");
            }
        }
        if (failure != null) {
            listener.close();
            server.stopShards(); // stops the shards that did start
            throw failure;
        }

        server.listener = listener;
        server.acceptor = new Thread(() -> acceptLoop(listener, router), "forgekv-acceptor");
        server.acceptor.setDaemon(true);
        server.acceptor.start();

        if (config.replicaofHost() != null) {
            String host = config.replicaofHost();
            int port = config.replicaofPort();
            router.send(0, new ShardMsg.Run(sh -> Replication.replicaof(sh, host, port)));
        }
        return server;
    }

    /** Start a server on an ephemeral loopback port (tests, benches). */
    public static Server startLocal(Config config) throws IOException {
        return start("127.0.0.1", 0, config);
    }

    public InetSocketAddress addr() {
        return addr;
    }

    public int port() {
        return addr.getPort();
    }

    public int shards() {
        return router.n;
    }

    /** Stop accepting, stop every shard (each fsyncs its AOF) and join them. */
    public synchronized void shutdown() {
        if (stopped) {
            return;
        }
        stopped = true;
        if (listener != null) {
            try {
                listener.close();
            } catch (IOException e) {
                // ignore
            }
        }
        if (acceptor != null) {
            try {
                acceptor.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        router.repl.stopLink();
        stopShards();
    }

    private void stopShards() {
        router.broadcast(ShardMsg.Shutdown::new);
        for (Thread t : shardThreads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        shardThreads.clear();
    }

    @Override
    public void close() {
        shutdown();
    }

    /** Accept connections and deal them out to the shards round-robin. */
    private static void acceptLoop(ServerSocketChannel listener, Router router) {
        long next = 0;
        while (listener.isOpen()) {
            SocketChannel s;
            try {
                s = listener.accept();
            } catch (ClosedChannelException e) {
                return;
            } catch (IOException e) {
                // e.g. EMFILE: back off instead of spinning.
                Log.warn("accept error: " + e);
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    return;
                }
                continue;
            }
            int target = (int) (next++ % router.n);
            if (!router.send(target, new ShardMsg.Conn(s))) {
                try {
                    s.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
}
