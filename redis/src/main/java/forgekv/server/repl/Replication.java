package forgekv.server.repl;

import forgekv.resp.Frame;
import forgekv.server.Outbox;
import forgekv.server.Router;
import forgekv.server.Shard;
import forgekv.server.ShardMsg;
import forgekv.storage.Clock;
import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import forgekv.util.Log;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Leader–follower replication.
 *
 * <h2>What is replicated</h2>
 *
 * <p>The replication stream is exactly the canonical command stream the AOF
 * records ({@link forgekv.server.aof.AofLog#feed}): time-independent
 * ({@code PXAT}, {@code PEXPIREAT}), conditional commands only when they
 * changed something, expiry as explicit {@code DEL}, and {@code FLUSHALL}
 * scoped per shard. A replica therefore applies it on a frozen clock and
 * never expires keys on its own (passive expiry). It simply hides expired
 * keys from readers until the leader's {@code DEL} arrives, so clock skew
 * cannot make the two diverge.
 *
 * <h2>Protocol (ForgeKV ↔ ForgeKV)</h2>
 *
 * <pre>
 * replica → PING                              ← +PONG
 * replica → REPLCONF listening-port &lt;port&gt;    ← +OK
 * replica → PSYNC &lt;replid&gt; &lt;offset&gt;           (or `PSYNC ? -1` the first time)
 *   ← +CONTINUE &lt;replid&gt;\r\n &lt;backlog bytes from offset&gt;...      partial resync
 *   ← +FULLRESYNC &lt;replid&gt; &lt;offset&gt;\r\n $&lt;len&gt;\r\n&lt;snapshot&gt;...  full resync
 * replica → REPLCONF ACK &lt;offset&gt;   (every second, and when asked via GETACK)
 * </pre>
 *
 * <p>The snapshot is a RESP command stream (like an AOF rewrite), not an RDB.
 *
 * <h2>Consistent snapshots without stopping the world</h2>
 *
 * <p>Each leader shard snapshots itself independently, between two of its
 * own commands. The hub then merges the parts:
 * <ul>
 *   <li>a write from shard <i>s</i> that arrives <b>before</b> <i>s</i>'s
 *       snapshot part is dropped for that replica (the snapshot already
 *       contains it);
 *   <li>a write that arrives <b>after</b> <i>s</i>'s part but before every
 *       shard has reported is appended to the payload;
 *   <li>once all parts are in, the payload is sent and the replica is
 *       <i>online</i> from that backlog offset on.
 * </ul>
 *
 * <p>Every shard's writes reach the hub in order, so each key's history is
 * seen exactly once and in order. A global pause is never needed.
 *
 * <h2>Backlog</h2>
 *
 * <p>The leader keeps the last {@code repl-backlog-size} bytes of the
 * stream. A replica that reconnects with a known {@code (replid, offset)}
 * still covered by the backlog resumes with {@code +CONTINUE} instead of a
 * full resync.
 */
public final class Replication {
    /**
     * Undelivered stream chunks queued per replica before it is dropped (it
     * then reconnects and resumes from the backlog).
     */
    public static final int REPLICA_QUEUE = 16 * 1024;
    /** Commands applied per batch on the replica side. */
    static final int APPLY_BATCH = 1024;
    static final long ACK_INTERVAL_MS = 1000;

    private static final Bytes GETACK =
            Bytes.of("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");

    /** Outcome of a replica's {@code PSYNC}. */
    public enum Registered { CONTINUE, FULL }

    private static final class ReplicaConn {
        final long id;
        final Outbox tx;
        final String ip;
        final int port;
        /** {@code null} once online; otherwise which shards' snapshot parts are in. */
        boolean[] got;
        ByteBuf payload;
        long ack;
        long ackMs;

        ReplicaConn(long id, Outbox tx, String ip, int port, boolean[] got, long ack) {
            this.id = id;
            this.tx = tx;
            this.ip = ip;
            this.port = port;
            this.got = got;
            this.payload = got == null ? null : new ByteBuf(4096);
            this.ack = ack;
            this.ackMs = Clock.nowMs();
        }

        boolean online() {
            return got == null;
        }
    }

    /** A pending {@code WAIT}. */
    private static final class Waiter {
        final long target;
        final int numreplicas;
        final Shard origin;
        final Consumer<Frame> cb;
        final AtomicBoolean done = new AtomicBoolean();

        Waiter(long target, int numreplicas, Shard origin, Consumer<Frame> cb) {
            this.target = target;
            this.numreplicas = numreplicas;
            this.origin = origin;
            this.cb = cb;
        }
    }

    private final int nShards;

    // ---- the hub (guarded by `hub`) ----
    private final Object hub = new Object();
    private String replid = newReplid();
    private final Backlog backlog;
    private final List<ReplicaConn> replicas = new ArrayList<>();
    private final List<Waiter> waiters = new ArrayList<>();
    private long nextId = 1;
    private long syncFull;
    private long syncPartialOk;
    private long syncPartialErr;

    // ---- follower side ----
    private final Object linkLock = new Object();
    private String linkHost;
    private int linkPort;
    private ReplicaLink linkTask;
    /** Replication id of the leader we last synced with. */
    String masterReplid;
    final AtomicBoolean isReplica = new AtomicBoolean();
    final AtomicBoolean linkUp = new AtomicBoolean();
    /** Follower: bytes of the leader's stream applied so far. */
    final AtomicLong linkOffset = new AtomicLong();
    final AtomicLong linkLastIo = new AtomicLong();

    public Replication(int nShards, int backlogSize) {
        this.nShards = nShards;
        this.backlog = new Backlog(backlogSize);
    }

    static String newReplid() {
        long x = Clock.nowMs() ^ (ProcessHandle.current().pid() << 32) ^ 0x9E3779B97F4A7C15L ^ System.nanoTime();
        StringBuilder id = new StringBuilder(48);
        while (id.length() < 40) {
            x ^= x << 13;
            x ^= x >>> 7;
            x ^= x << 17;
            id.append(String.format("%016x", x));
        }
        return id.substring(0, 40);
    }

    public boolean isReplica() {
        return isReplica.get();
    }

    public long masterOffset() {
        synchronized (hub) {
            return backlog.end;
        }
    }

    String replid() {
        synchronized (hub) {
            return replid;
        }
    }

    /**
     * Append stream bytes. {@code shard} is the origin ({@code -1} = control
     * traffic such as {@code REPLCONF GETACK}, which syncing replicas don't need).
     */
    public void feed(int shard, Bytes bytes) {
        synchronized (hub) {
            backlog.push(bytes.array());
            for (Iterator<ReplicaConn> it = replicas.iterator(); it.hasNext();) {
                ReplicaConn r = it.next();
                if (r.online()) {
                    if (r.tx.offer(bytes) != Outbox.Offer.OK) {
                        Log.warn("replica " + r.ip + ":" + r.port + " can't keep up; dropping it (it will resync)");
                        r.tx.close();
                        it.remove();
                    }
                } else if (shard >= 0 && r.got[shard]) {
                    r.payload.put(bytes);
                }
            }
        }
    }

    /** One shard's snapshot for {@code replica} (see the class docs). */
    public void snapshotPart(int shard, long replica, Bytes bytes) {
        synchronized (hub) {
            ReplicaConn r = null;
            for (ReplicaConn c : replicas) {
                if (c.id == replica) {
                    r = c;
                    break;
                }
            }
            if (r == null || r.online() || r.got[shard]) {
                return; // replica went away meanwhile, or nothing to do
            }
            r.got[shard] = true;
            r.payload.put(bytes);
            for (boolean g : r.got) {
                if (!g) {
                    return;
                }
            }
            long end = backlog.end;
            Bytes payload = r.payload.take();
            Bytes head = Bytes.of("+FULLRESYNC " + replid + " " + end + "\r\n$" + payload.length() + "\r\n");
            boolean sent = r.tx.offer(head) == Outbox.Offer.OK && r.tx.offer(payload) == Outbox.Offer.OK;
            r.got = null;
            r.payload = null;
            r.ack = 0;
            if (sent) {
                Log.info("replica " + r.ip + ":" + r.port + " full sync done at offset " + end);
            } else {
                r.tx.close();
                replicas.remove(r);
            }
        }
    }

    public Registered register(long id, Outbox tx, String ip, int port, String theirReplid, long offset) {
        synchronized (hub) {
            if (theirReplid.equals(replid) && offset >= 0) {
                Bytes data = backlog.since(offset);
                if (data != null) {
                    Bytes head = Bytes.of("+CONTINUE " + replid + "\r\n");
                    if (tx.offer(head) == Outbox.Offer.OK
                            && (data.isEmpty() || tx.offer(data) == Outbox.Offer.OK)) {
                        syncPartialOk++;
                        replicas.add(new ReplicaConn(id, tx, ip, port, null, offset));
                        return Registered.CONTINUE;
                    }
                }
            }
            if (!theirReplid.equals("?")) {
                syncPartialErr++;
            }
            syncFull++;
            replicas.add(new ReplicaConn(id, tx, ip, port, new boolean[nShards], 0));
            return Registered.FULL;
        }
    }

    public long nextReplicaId() {
        synchronized (hub) {
            nextId++;
            return nextId;
        }
    }

    public void ack(long id, long offset) {
        synchronized (hub) {
            for (ReplicaConn r : replicas) {
                if (r.id == id) {
                    r.ack = Math.max(r.ack, offset);
                    r.ackMs = Clock.nowMs();
                }
            }
            for (Iterator<Waiter> it = waiters.iterator(); it.hasNext();) {
                Waiter w = it.next();
                int n = countAckedLocked(w.target);
                if (n >= w.numreplicas) {
                    it.remove();
                    if (w.done.compareAndSet(false, true)) {
                        w.origin.execute(() -> w.cb.accept(Frame.integer(n)));
                    }
                }
            }
        }
    }

    public void remove(long id) {
        synchronized (hub) {
            replicas.removeIf(r -> r.id == id);
        }
    }

    /** {@code CLIENT KILL TYPE replica}: drop every replica connection. */
    public int killReplicas() {
        synchronized (hub) {
            int n = replicas.size();
            for (ReplicaConn r : replicas) {
                r.tx.close();
            }
            replicas.clear();
            return n;
        }
    }

    private int countAckedLocked(long target) {
        int n = 0;
        for (ReplicaConn r : replicas) {
            if (r.online() && r.ack >= target) {
                n++;
            }
        }
        return n;
    }

    private int countAcked(long target) {
        synchronized (hub) {
            return countAckedLocked(target);
        }
    }

    /**
     * {@code WAIT numreplicas timeout}: reply (on {@code origin}'s thread)
     * once that many replicas acknowledged everything written so far, or the
     * timeout (ms, 0 = forever) expires.
     */
    public void waitFor(int numreplicas, long timeoutMs, Shard origin, Consumer<Frame> cb) {
        if (isReplica()) {
            cb.accept(Frame.error("ERR WAIT cannot be used with replica instances."));
            return;
        }
        long target;
        Waiter w = null;
        int already;
        synchronized (hub) {
            target = backlog.end;
            already = countAckedLocked(target);
            if (already < numreplicas) {
                w = new Waiter(target, numreplicas, origin, cb);
                waiters.add(w);
            }
        }
        if (w == null) {
            cb.accept(Frame.integer(already));
            return;
        }
        Waiter waiter = w;
        // Ask replicas to report their offset right away.
        feed(-1, GETACK);
        if (timeoutMs > 0) {
            origin.schedule(timeoutMs, () -> {
                if (waiter.done.compareAndSet(false, true)) {
                    synchronized (hub) {
                        waiters.remove(waiter);
                    }
                    cb.accept(Frame.integer(countAcked(target)));
                }
            });
        }
    }

    /** The {@code # Replication} section of {@code INFO}. */
    public String info() {
        StringBuilder s = new StringBuilder();
        long now = Clock.nowMs();
        if (isReplica()) {
            String host;
            int port;
            synchronized (linkLock) {
                host = linkHost == null ? "" : linkHost;
                port = linkPort;
            }
            boolean up = linkUp.get();
            s.append("role:slave\r\nmaster_host:").append(host)
                    .append("\r\nmaster_port:").append(port)
                    .append("\r\nmaster_link_status:").append(up ? "up" : "down")
                    .append("\r\nmaster_last_io_seconds_ago:")
                    .append(up ? Math.max(0, now - linkLastIo.get()) / 1000 : 0)
                    .append("\r\nslave_repl_offset:").append(linkOffset.get())
                    .append("\r\nslave_read_only:1\r\n");
        } else {
            s.append("role:master\r\n");
        }
        synchronized (hub) {
            s.append("connected_slaves:").append(replicas.size()).append("\r\n");
            for (int i = 0; i < replicas.size(); i++) {
                ReplicaConn r = replicas.get(i);
                s.append("slave").append(i).append(":ip=").append(r.ip).append(",port=").append(r.port)
                        .append(",state=").append(r.online() ? "online" : "wait_bgsave")
                        .append(",offset=").append(r.ack)
                        .append(",lag=").append(Math.max(0, now - r.ackMs) / 1000).append("\r\n");
            }
            s.append("master_replid:").append(replid)
                    .append("\r\nmaster_repl_offset:").append(backlog.end)
                    .append("\r\nrepl_backlog_active:").append(backlog.end > 0 ? 1 : 0)
                    .append("\r\nrepl_backlog_size:").append(backlog.cap)
                    .append("\r\nrepl_backlog_first_byte_offset:").append(backlog.start())
                    .append("\r\nrepl_backlog_histlen:").append(backlog.histlen())
                    .append("\r\nsync_full:").append(syncFull)
                    .append("\r\nsync_partial_ok:").append(syncPartialOk)
                    .append("\r\nsync_partial_err:").append(syncPartialErr)
                    .append("\r\n");
        }
        return s.toString();
    }

    // -----------------------------------------------------------------------
    // Follower side
    // -----------------------------------------------------------------------

    /** {@code REPLICAOF host port} ({@code host != null}) / {@code REPLICAOF NO ONE}. Runs on {@code shard}. */
    public static void replicaof(Shard shard, String host, int port) {
        Router router = shard.router;
        Replication repl = router.repl;
        synchronized (repl.linkLock) {
            if (repl.linkTask != null) {
                repl.linkTask.shutdown();
                repl.linkTask = null;
            }
            repl.linkUp.set(false);
            if (host == null) {
                repl.linkHost = null;
                if (repl.isReplica.getAndSet(false)) {
                    // Promoted: a new history starts here, so our own replicas
                    // must not partially resync across the switch.
                    synchronized (repl.hub) {
                        repl.replid = newReplid();
                    }
                    repl.masterReplid = null;
                    setPassiveExpiry(shard, false);
                    Log.info("promoted to master");
                }
            } else {
                Log.info("replicating from " + host + ":" + port);
                repl.isReplica.set(true);
                // Set it locally right away; other shards get it before any
                // replicated command (mailboxes are FIFO).
                setPassiveExpiry(shard, true);
                repl.linkHost = host;
                repl.linkPort = port;
                repl.linkTask = new ReplicaLink(shard, host, port);
                repl.linkTask.start();
            }
        }
    }

    private static void setPassiveExpiry(Shard shard, boolean on) {
        shard.db().setPassiveExpiry(on);
        Router router = shard.router;
        for (int s = 0; s < router.n; s++) {
            if (s != shard.idx) {
                router.send(s, new ShardMsg.Run(sh -> sh.db().setPassiveExpiry(on)));
            }
        }
    }

    String cachedMasterReplid() {
        synchronized (linkLock) {
            return masterReplid;
        }
    }

    void setMasterReplid(ReplicaLink who, String id) {
        synchronized (linkLock) {
            if (linkTask == who) {
                masterReplid = id;
            }
        }
    }

    /** Stop the follower link, if any (server shutdown). */
    public void stopLink() {
        synchronized (linkLock) {
            if (linkTask != null) {
                linkTask.shutdown();
                linkTask = null;
            }
        }
    }

    static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }
}
