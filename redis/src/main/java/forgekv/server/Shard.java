package forgekv.server;

import forgekv.resp.Frame;
import forgekv.server.aof.Aof;
import forgekv.server.aof.AofLog;
import forgekv.server.aof.FsyncPolicy;
import forgekv.server.aof.IoBackend;
import forgekv.server.cmd.Commands;
import forgekv.storage.Clock;
import forgekv.storage.Db;
import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import forgekv.util.Log;
import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One shard: a thread that exclusively owns a slice of the keyspace.
 *
 * <p>Each shard runs its own single-threaded event loop on an NIO
 * {@link Selector} (thread-per-core, shared-nothing). Inside a shard nothing
 * is locked: the keyspace is touched only by this thread and every command
 * runs to completion before the next one starts, so every command is atomic
 * on its shard.
 *
 * <p>A shard does three kinds of work, all on its own thread:
 * <ul>
 *   <li>serves the client connections handed to it by the acceptor,
 *   <li>executes sub-batches other shards send to its mailbox ({@link ShardMsg}),
 *   <li>runs its own background jobs: active expiry, AOF fsync, and io_uring
 *       completions.
 * </ul>
 *
 * <p>Every write's canonical effect (see {@link AofLog#feed}) goes into
 * {@code effects}, and {@link #commit()} hands it to the AOF and the
 * replication hub in one go. One commit covers everything executed since the
 * last one, so persistence is naturally group-committed.
 */
public final class Shard implements Runnable, Executor {
    /** Interval of the active-expiry task (Redis default {@code hz 10}). */
    private static final long ACTIVE_EXPIRE_INTERVAL_MS = 100;
    /** How often {@code appendfsync everysec} flushes to disk. */
    private static final long FSYNC_INTERVAL_MS = 1000;
    /** Mailbox messages handled per wake-up before committing. */
    private static final int MAILBOX_BATCH = 256;
    /**
     * Event-loop passes a group commit waits before it syncs.
     *
     * <p>Why two passes: with {@code appendfsync always} every client waits
     * for the same fsync, so they all reply and re-send at about the same
     * time. With a single pass the clients split into two cohorts that
     * alternate between the in-flight fsync and the next one, halving the
     * group size. The second pass catches the stragglers.
     */
    private static final int GROUP_COMMIT_PASSES = 2;

    /** AOF settings for one shard. */
    public record ShardAof(Path path, FsyncPolicy fsync, IoBackend backend) {}

    private record Waiter(long seq, long order, Runnable task) {}

    private record Timer(long at, long order, Runnable task) {}

    public final int idx;
    public final Router router;
    private final Db db;
    private final ShardAof aofCfg;
    private Aof aof;
    /** Canonical effects of commands executed since the last commit. */
    private final ByteBuf effects = new ByteBuf(16 * 1024);
    /** Effects are being recorded (AOF on, or a replica is attached). */
    private boolean propagate;
    /** Effects are streamed to the replication hub. */
    private boolean replOn;
    /** Highest AOF sequence known to be durable. */
    private long durable;
    private final PriorityQueue<Waiter> durableWaiters = new PriorityQueue<>(
            (a, b) -> a.seq != b.seq ? Long.compare(a.seq, b.seq) : Long.compare(a.order, b.order));
    /** A group commit is wanted (counts event-loop passes in {@code syncPasses}). */
    private boolean syncWanted;
    private int syncPasses;

    long commands;
    long connectedClients;
    long totalConnections;

    private final ConcurrentLinkedQueue<ShardMsg> mailbox = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean needWakeup = new AtomicBoolean();
    private volatile boolean stopped;
    private final Selector selector;
    private final PriorityQueue<Timer> timers = new PriorityQueue<>(
            (a, b) -> a.at != b.at ? Long.compare(a.at, b.at) : Long.compare(a.order, b.order));
    private long order;
    final Set<Connection> connections = new HashSet<>();
    final CompletableFuture<Void> ready = new CompletableFuture<>();

    Shard(int idx, Router router, Db db, ShardAof aofCfg) throws IOException {
        this.idx = idx;
        this.router = router;
        this.db = db;
        this.aofCfg = aofCfg;
        this.selector = Selector.open();
    }

    /** This shard's keyspace. Only to be touched from this shard's thread. */
    public Db db() {
        return db;
    }

    // -----------------------------------------------------------------------
    // Mailbox
    // -----------------------------------------------------------------------

    /** Post a message to this shard. {@code false} if it has stopped. */
    public boolean post(ShardMsg m) {
        if (stopped) {
            return false;
        }
        mailbox.add(m);
        if (needWakeup.compareAndSet(true, false)) {
            selector.wakeup();
        }
        return true;
    }

    /** Run {@code r} on this shard's thread. */
    @Override
    public void execute(Runnable r) {
        post(new ShardMsg.Run(sh -> r.run()));
    }

    /** Run {@code r} on this shard's thread after {@code delayMs}. Shard thread only. */
    public void schedule(long delayMs, Runnable r) {
        timers.add(new Timer(Clock.nowMs() + delayMs, order++, r));
    }

    private void scheduleRepeating(long intervalMs, Runnable r) {
        schedule(intervalMs, new Runnable() {
            @Override
            public void run() {
                r.run();
                schedule(intervalMs, this);
            }
        });
    }

    // -----------------------------------------------------------------------
    // Execution, effects and durability
    // -----------------------------------------------------------------------

    /**
     * Execute one command on this shard's keyspace, recording its effect.
     * {@code spec} skips the command-table lookup when the caller already did it.
     */
    public Frame exec(Bytes[] args, Mode mode, Commands.Spec spec) {
        Frame reply;
        try {
            reply = Commands.executeWith(db, mode.now(), args, spec);
        } catch (RuntimeException e) {
            Log.error("command " + args[0] + " failed: " + e);
            reply = Frame.error("ERR internal error");
        }
        if (propagate) {
            AofLog.feed(effects, db, idx, router.n, args, reply);
        }
        return reply;
    }

    /** Record {@code DEL}s for keys removed by expiry since the last call. */
    void logExpired() {
        if (!propagate) {
            return;
        }
        for (Bytes key : db.drainExpired()) {
            AofLog.encodeCommand(effects, AofLog.DEL, key);
        }
    }

    /**
     * Hand recorded effects to replication and the AOF.
     *
     * <p>Returns the AOF sequence a reply must wait for: the latest one, not
     * just this caller's, so no client ever reads data that is not yet as
     * durable as the fsync policy promises. 0 = nothing to wait for.
     */
    public long commit() {
        if (!effects.isEmpty()) {
            Bytes fx = effects.take();
            if (replOn) {
                router.repl.feed(idx, fx);
            }
            if (aof != null) {
                aof.appendRaw(fx);
            }
        }
        if (aof == null) {
            return 0;
        }
        long seq;
        try {
            seq = aof.flush();
        } catch (IOException e) {
            // Carrying on after acknowledging writes that were not persisted
            // would silently break the durability contract (Redis does the same).
            fatal("can't write to the append-only file: " + e);
            return 0;
        }
        publishDurable(aof.durableSeq());
        if (aof.needsGroupSync()) {
            syncWanted = true;
        }
        return seq;
    }

    private static void fatal(String msg) {
        Log.error(msg + "; exiting");
        Runtime.getRuntime().halt(1);
    }

    /** One {@code fdatasync} (or io_uring chain) covering every write since the last one. */
    private void groupSync() {
        if (aof == null) {
            return;
        }
        try {
            aof.groupSync();
        } catch (IOException e) {
            fatal("can't fsync the append-only file: " + e);
        }
        publishDurable(aof.durableSeq());
    }

    private void publishDurable(long d) {
        if (d > durable) {
            durable = d;
        }
    }

    public boolean isDurable(long seq) {
        return seq == 0 || durable >= seq;
    }

    /** Run {@code r} (on this thread) once AOF sequence {@code seq} is durable. */
    public void whenDurable(long seq, Runnable r) {
        if (isDurable(seq)) {
            r.run();
        } else {
            durableWaiters.add(new Waiter(seq, order++, r));
        }
    }

    private void runDurableWaiters() {
        while (!durableWaiters.isEmpty() && durableWaiters.peek().seq <= durable) {
            durableWaiters.poll().task.run();
        }
    }

    /** io_uring completions are ready. */
    private void onAofCompletion() {
        if (aof == null) {
            return;
        }
        try {
            aof.onCompletion();
        } catch (IOException e) {
            fatal("can't write to the append-only file: " + e);
        }
        publishDurable(aof.durableSeq());
        // Writes queued behind the finished chain go out as the next group.
        if (aof.needsGroupSync()) {
            syncWanted = true;
        }
    }

    public ShardStats stats() {
        return new ShardStats(
                db.size(),
                db.expiresSize(),
                db.stats(),
                commands,
                connectedClients,
                totalConnections,
                aof == null ? 0 : aof.size(),
                aof == null ? 0 : aof.fsyncs(),
                aof == null ? "none" : aof.backendName());
    }

    /** {@code BGREWRITEAOF} for this shard. Returns an error message, or {@code null}. */
    String rewriteAof() {
        logExpired();
        commit();
        if (aof == null) {
            return "append only file is not enabled";
        }
        try {
            aof.rewrite(db, Clock.nowMs());
        } catch (IOException e) {
            return e.toString();
        }
        publishDurable(aof.durableSeq());
        return null;
    }

    /**
     * Produce this shard's slice of a replica's full-sync payload, and start
     * streaming every later write to the hub.
     *
     * <p>Runs between two commands, so the snapshot and the stream that
     * follows it meet exactly: nothing is missed and nothing is applied twice.
     */
    void replSnapshot(long replica) {
        logExpired();
        commit();
        replOn = true;
        propagate = true;
        db.setTrackExpired(true);
        ByteBuf buf = new ByteBuf(4096);
        AofLog.snapshotInto(buf, db, Clock.nowMs());
        router.repl.snapshotPart(idx, replica, buf.toBytes());
    }

    private void finalSync() {
        logExpired();
        commit();
        if (aof != null) {
            try {
                aof.syncNow();
            } catch (IOException e) {
                Log.error("final AOF sync failed: " + e);
            }
        }
    }

    // -----------------------------------------------------------------------
    // The event loop
    // -----------------------------------------------------------------------

    /** Body of the shard thread. {@link #ready} reports whether the shard came up. */
    @Override
    public void run() {
        if (aofCfg != null) {
            try {
                aof = Aof.open(aofCfg.path(), aofCfg.fsync(), aofCfg.backend());
            } catch (IOException | RuntimeException e) {
                stopped = true;
                closeSelector();
                ready.completeExceptionally(e);
                return;
            }
        }
        propagate = aof != null;
        db.setTrackExpired(propagate);
        scheduleRepeating(ACTIVE_EXPIRE_INTERVAL_MS, this::expireTick);
        if (aof != null) {
            if (aof.fsyncPolicy() == FsyncPolicy.EVERYSEC) {
                scheduleRepeating(FSYNC_INTERVAL_MS, aof::tickFsync);
            }
            if (aof.isUring()) {
                aof.startCompletionWatcher(() -> post(new ShardMsg.Run(Shard::onAofCompletion)));
            }
        }
        ready.complete(null);

        try {
            loop();
        } catch (IOException | RuntimeException e) {
            Log.error("shard " + idx + " event loop failed: " + e);
        } finally {
            stopped = true;
            finalSync();
            for (Connection c : new ArrayList<>(connections)) {
                c.close();
            }
            if (aof != null) {
                aof.close();
            }
            closeSelector();
        }
    }

    private void closeSelector() {
        try {
            selector.close();
        } catch (IOException e) {
            // ignore
        }
    }

    private void loop() throws IOException {
        while (true) {
            needWakeup.set(true);
            boolean immediate = syncWanted || !mailbox.isEmpty()
                    || (!durableWaiters.isEmpty() && durableWaiters.peek().seq <= durable);
            if (immediate) {
                selector.selectNow();
            } else {
                long timeout = timers.isEmpty() ? 0 : Math.max(1, timers.peek().at - Clock.nowMs());
                selector.select(timeout);
            }
            needWakeup.set(false);

            handleIo();
            runDurableWaiters();
            if (drainMailbox()) {
                return;
            }
            runDurableWaiters();
            runTimers();
            if (syncWanted && ++syncPasses >= GROUP_COMMIT_PASSES) {
                syncPasses = 0;
                syncWanted = false;
                groupSync();
            }
            runDurableWaiters();
        }
    }

    private void handleIo() {
        Set<SelectionKey> keys = selector.selectedKeys();
        if (keys.isEmpty()) {
            return;
        }
        for (Iterator<SelectionKey> it = keys.iterator(); it.hasNext();) {
            SelectionKey k = it.next();
            it.remove();
            Connection c = (Connection) k.attachment();
            try {
                if (!k.isValid()) {
                    continue;
                }
                int ready = k.readyOps();
                if ((ready & SelectionKey.OP_WRITE) != 0) {
                    c.onWritable();
                }
                if ((ready & SelectionKey.OP_READ) != 0 && k.isValid()) {
                    c.onReadable();
                }
            } catch (CancelledKeyException e) {
                c.close();
            } catch (RuntimeException e) {
                Log.error("connection error: " + e);
                c.close();
            }
        }
    }

    private record PendingReply(ShardMsg.Exec exec, Frame[] frames) {}

    /** Handle a batch of mailbox messages. Returns {@code true} on shutdown. */
    private boolean drainMailbox() {
        List<PendingReply> replies = null;
        boolean shutdown = false;
        int handled = 0;
        ShardMsg m;
        while (handled < MAILBOX_BATCH && (m = mailbox.poll()) != null) {
            handled++;
            switch (m) {
                case ShardMsg.Exec e -> {
                    Frame[] frames = new Frame[e.cmds().size()];
                    for (int i = 0; i < frames.length; i++) {
                        frames[i] = exec(e.cmds().get(i), e.mode(), null);
                    }
                    if (replies == null) {
                        replies = new ArrayList<>();
                    }
                    replies.add(new PendingReply(e, frames));
                }
                case ShardMsg.Conn c -> accept(c.channel());
                case ShardMsg.Run r -> {
                    try {
                        r.task().accept(this);
                    } catch (RuntimeException ex) {
                        Log.error("shard task failed: " + ex);
                    }
                }
                case ShardMsg.Shutdown s -> shutdown = true;
            }
            if (shutdown) {
                break;
            }
        }
        if (handled == 0) {
            return false;
        }
        // One commit (one AOF write/fsync) for everything handled above.
        long seq = commit();
        if (replies != null) {
            for (PendingReply p : replies) {
                ShardMsg.Exec e = p.exec();
                whenDurable(seq, () -> e.origin().post(new ShardMsg.Run(o -> e.reply().accept(p.frames()))));
            }
        }
        return shutdown;
    }

    private void runTimers() {
        long now = Clock.nowMs();
        while (!timers.isEmpty() && timers.peek().at <= now) {
            Timer t = timers.poll();
            try {
                t.task.run();
            } catch (RuntimeException e) {
                Log.error("timer task failed: " + e);
            }
        }
    }

    private void expireTick() {
        int n = db.activeExpireCycle(Clock.nowMs());
        if (n > 0) {
            if (Log.enabled(Log.Level.DEBUG)) {
                Log.debug("shard " + idx + ": active expiry cycle reclaimed " + n);
            }
            logExpired();
            commit();
        }
    }

    private void accept(SocketChannel ch) {
        try {
            ch.configureBlocking(false);
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
            Connection c = new Connection(this, ch);
            c.key = ch.register(selector, SelectionKey.OP_READ, c);
            connections.add(c);
            connectedClients++;
            totalConnections++;
        } catch (IOException e) {
            Log.warn("dropping connection: " + e);
            try {
                ch.close();
            } catch (IOException ex) {
                // ignore
            }
        }
    }
}
