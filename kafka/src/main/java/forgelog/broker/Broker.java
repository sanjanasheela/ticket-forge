package forgelog.broker;

import forgelog.log.Record;
import forgelog.log.Region;
import forgelog.protocol.ApiKey;
import forgelog.protocol.Errors;
import forgelog.protocol.Messages.BrokerInfo;
import forgelog.protocol.Messages.ChaosRequest;
import forgelog.protocol.Messages.CreateTopicRequest;
import forgelog.protocol.Messages.FetchPartition;
import forgelog.protocol.Messages.FetchRequest;
import forgelog.protocol.Messages.FetchResult;
import forgelog.protocol.Messages.GroupStatus;
import forgelog.protocol.Messages.HeartbeatRequest;
import forgelog.protocol.Messages.JoinGroupRequest;
import forgelog.protocol.Messages.JoinGroupResponse;
import forgelog.protocol.Messages.LeaveGroupRequest;
import forgelog.protocol.Messages.MetadataRequest;
import forgelog.protocol.Messages.MetadataResponse;
import forgelog.protocol.Messages.OffsetCommitRequest;
import forgelog.protocol.Messages.OffsetFetchRequest;
import forgelog.protocol.Messages.OffsetFetchResponse;
import forgelog.protocol.Messages.PartitionInfo;
import forgelog.protocol.Messages.PartitionOffset;
import forgelog.protocol.Messages.ProducePartition;
import forgelog.protocol.Messages.ProduceRecord;
import forgelog.protocol.Messages.ProduceRequest;
import forgelog.protocol.Messages.ProduceResponse;
import forgelog.protocol.Messages.ProduceResult;
import forgelog.protocol.Messages.SimpleResponse;
import forgelog.protocol.Messages.StatusResponse;
import forgelog.protocol.Messages.TopicInfo;
import forgelog.protocol.RaftCodec;
import forgelog.protocol.TopicPartition;
import forgelog.protocol.Wire;
import forgelog.protocol.Wire.Reader;
import forgelog.protocol.Wire.Writer;
import forgelog.raft.NotLeaderException;
import forgelog.raft.RaftMessage;
import forgelog.raft.RaftNode;
import forgelog.util.Log;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * A ForgeLog broker.
 *
 * <p>Everything runs on <b>one event-loop thread</b>: client and peer I/O
 * (NIO), every Raft group's timers and messages, the logs, the group
 * coordinator. No locks, no data races, and every state transition happens
 * in a well-defined order.
 *
 * <p>Each pass of the loop:
 * <ol>
 *   <li>handles ready sockets (requests may propose entries to Raft groups);
 *   <li>ticks the Raft groups (elections, heartbeats) and runs due timers;
 *   <li>sends the pending appends of every group (one message per group per
 *       follower, however many records were proposed in this pass);
 *   <li><b>group commit</b>: flushes (and, by default, fsyncs) every log
 *       written to, then tells Raft what is durable, which may commit entries
 *       and complete waiting produce requests;
 *   <li>only then sends append acknowledgements, so a follower never
 *       acknowledges an entry it could lose in a crash.
 * </ol>
 *
 * <p>Replication: every topic partition is its own Raft group whose log is
 * the partition log, replicated on every broker of the cluster. Two internal
 * groups hold the cluster metadata ({@link MetadataReplica}) and committed
 * consumer offsets ({@link OffsetsReplica}).
 */
public final class Broker implements AutoCloseable {
    private static final long TICK_MS = 10;
    private static final Pattern TOPIC_NAME = Pattern.compile("[A-Za-z0-9._-]{1,200}");

    private record Timer(long at, long seq, Runnable task) {}

    private record Deferred(int to, RaftMessage msg) {}

    /** A fetch parked until data arrives or its wait runs out. */
    private static final class FetchWait {
        final ClientConn conn;
        final int corr;
        final FetchRequest req;
        final long deadline;
        boolean done;
        final List<PartitionReplica> parkedOn = new ArrayList<>();

        FetchWait(ClientConn conn, int corr, FetchRequest req, long deadline) {
            this.conn = conn;
            this.corr = corr;
            this.req = req;
            this.deadline = deadline;
        }
    }

    private final BrokerConfig cfg;
    private final Selector selector;
    private final ServerSocketChannel server;
    private final Map<Integer, PeerLink> links = new HashMap<>();
    private final Map<Integer, Replica> groups = new TreeMap<>();
    private Replica[] groupArray = new Replica[0];
    private final Map<TopicPartition, PartitionReplica> partitions = new HashMap<>();
    private final Set<ClientConn> conns = new HashSet<>();
    private MetadataReplica metadata;
    private OffsetsReplica offsets;
    private final GroupCoordinator coordinator = new GroupCoordinator(this);
    private final List<Deferred> deferred = new ArrayList<>();
    private Set<Integer> blocked = Set.of();
    private final PriorityQueue<Timer> timers =
            new PriorityQueue<>(Comparator.comparingLong(Timer::at).thenComparingLong(Timer::seq));
    private long timerSeq;
    private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final Map<PartitionReplica, List<FetchWait>> fetchWaits = new HashMap<>();
    private Thread thread;
    private volatile boolean running = true;
    private final ExecutorService fsyncPool = Executors.newFixedThreadPool(8, r -> {
        Thread t = new Thread(r, "forgelog-fsync");
        t.setDaemon(true);
        return t;
    });

    private final ClientConn.FrameHandler frames = new ClientConn.FrameHandler() {
        @Override
        public void onFrame(ClientConn conn, ByteBuffer frame) {
            handleFrame(conn, frame);
        }

        @Override
        public void onClose(ClientConn conn) {
            conns.remove(conn);
            coordinator.onConnectionClosed(conn);
        }
    };

    private Broker(BrokerConfig cfg) throws IOException {
        this.cfg = cfg;
        this.selector = Selector.open();
        this.server = ServerSocketChannel.open();
        server.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        server.bind(cfg.self(), 1024);
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT, server);
    }

    /** Open the data directory, recover, and start serving. */
    public static Broker start(BrokerConfig cfg) throws IOException {
        Broker b = new Broker(cfg);
        try {
            b.init();
        } catch (IOException | RuntimeException e) {
            b.server.close();
            b.selector.close();
            throw e;
        }
        b.thread = new Thread(b::run, "forgelog-broker-" + cfg.nodeId());
        b.thread.start();
        Log.info("broker " + cfg.nodeId() + " listening on " + cfg.self() + " (cluster " + cfg.cluster().keySet()
                + ", fsync " + cfg.fsync() + ")");
        return b;
    }

    BrokerConfig config() {
        return cfg;
    }

    public int nodeId() {
        return cfg.nodeId();
    }

    public InetSocketAddress address() {
        return cfg.self();
    }

    static long now() {
        return System.nanoTime() / 1_000_000;
    }

    private void init() throws IOException {
        Files.createDirectories(cfg.dataDir());
        for (Map.Entry<Integer, InetSocketAddress> e : cfg.cluster().entrySet()) {
            if (e.getKey() != cfg.nodeId()) {
                links.put(e.getKey(), new PeerLink(e.getKey(), e.getValue(), selector));
            }
        }
        metadata = new MetadataReplica(this, cfg.dataDir().resolve("__metadata"));
        register(metadata);
        offsets = new OffsetsReplica(this, cfg.dataDir().resolve("__offsets"));
        register(offsets);
        long now = now();
        metadata.start(now); // replays known-committed topic creations, opening their partitions
        offsets.start(now);
        repeat(0, 100, () -> {
            long t = now();
            for (PeerLink l : links.values()) {
                l.maintain(t);
            }
            if (coordinator.active()) {
                coordinator.tick(t);
            }
        });
        repeat(1000, 1000, () -> {
            for (Replica r : groupArray) {
                r.maybeWriteHint();
            }
        });
    }

    private void register(Replica r) {
        groups.put(r.groupId, r);
        groupArray = groups.values().toArray(new Replica[0]);
    }

    /** Applied on every broker when a topic creation commits. */
    void onTopicCreated(MetadataReplica.TopicMeta t) {
        for (int p = 0; p < t.partitions(); p++) {
            TopicPartition tp = new TopicPartition(t.name(), p);
            try {
                PartitionReplica r = new PartitionReplica(this, t.firstGroupId() + p, tp,
                        cfg.dataDir().resolve("topics").resolve(tp.toString()));
                partitions.put(tp, r);
                register(r);
                r.start(now());
            } catch (IOException e) {
                throw new UncheckedIOException("can't open partition " + tp, e);
            }
        }
        Log.info("topic " + t.name() + " (" + t.partitions() + " partitions) created on broker " + cfg.nodeId());
    }

    int partitionCount(String topic) {
        MetadataReplica.TopicMeta t = metadata.topics().get(topic);
        return t == null ? 0 : t.partitions();
    }

    void onRoleChange(Replica r, RaftNode.Role role, long term, int leaderId) {
        if (role == RaftNode.Role.LEADER) {
            Log.info("broker " + cfg.nodeId() + " is now leader of " + r.name + " (term " + term + ")");
        }
        if (r == offsets) {
            coordinator.setActive(role == RaftNode.Role.LEADER);
        }
        if (r instanceof PartitionReplica p && role != RaftNode.Role.LEADER) {
            wakeFetchers(p);
        }
    }

    // -----------------------------------------------------------------------
    // the event loop
    // -----------------------------------------------------------------------

    private void schedule(long delayMs, Runnable task) {
        timers.add(new Timer(now() + delayMs, timerSeq++, task));
    }

    private void repeat(long firstDelayMs, long everyMs, Runnable task) {
        schedule(firstDelayMs, new Runnable() {
            @Override
            public void run() {
                task.run();
                schedule(everyMs, this);
            }
        });
    }

    private void run() {
        long nextTick = now();
        try {
            while (running) {
                long now = now();
                long due = nextTick;
                for (Replica r : groupArray) {
                    due = Math.min(due, r.raft.nextTimeout());
                }
                long wait = Math.min(due - now, timers.isEmpty() ? TICK_MS : timers.peek().at() - now);
                if (!tasks.isEmpty() || wait <= 0) {
                    selector.selectNow();
                } else {
                    selector.select(Math.max(1, Math.min(wait, TICK_MS)));
                }
                for (Iterator<SelectionKey> it = selector.selectedKeys().iterator(); it.hasNext();) {
                    SelectionKey k = it.next();
                    it.remove();
                    handleKey(k);
                }
                Runnable task;
                while ((task = tasks.poll()) != null) {
                    task.run();
                }
                now = now();
                // Every group at its own deadline (exact election timeouts); all of them
                // every TICK_MS as well, for check-quorum.
                boolean all = now >= nextTick;
                for (Replica r : groupArray) {
                    if (all || now >= r.raft.nextTimeout()) {
                        r.raft.tick(now);
                    }
                }
                if (all) {
                    nextTick = now + TICK_MS;
                }
                while (!timers.isEmpty() && timers.peek().at() <= now) {
                    timers.poll().task().run();
                }
                for (Replica r : groupArray) {
                    r.raft.flush(now);
                }
                groupCommit();
                // Only now may followers acknowledge what they appended.
                for (Deferred d : deferred) {
                    sendNow(d.to(), d.msg());
                }
                deferred.clear();
            }
        } catch (IOException | RuntimeException e) {
            Log.error("broker " + cfg.nodeId() + " stopped: " + e);
            if (Log.enabled(Log.Level.DEBUG)) {
                e.printStackTrace();
            }
        } finally {
            shutdownInternal();
        }
    }

    /**
     * Group commit: one fsync per written log covers everything this pass;
     * the logs are fsynced in parallel, so a broker hosting many partitions
     * pays for one fsync latency, not one per partition.
     */
    private void groupCommit() throws IOException {
        List<Replica> dirty = new ArrayList<>();
        List<Callable<Void>> work = new ArrayList<>();
        for (Replica r : groupArray) {
            if (r.log.isDirty()) {
                r.log.write();
                dirty.add(r);
                if (cfg.fsync()) {
                    work.add(() -> {
                        r.log.sync();
                        return null;
                    });
                }
            }
            if (r.hardState.isDirty()) {
                FileHardState hs = r.hardState;
                work.add(() -> {
                    hs.flush(); // terms and votes are always fsynced: Raft safety depends on them
                    return null;
                });
            }
        }
        if (!work.isEmpty()) {
            if (work.size() == 1) {
                try {
                    work.getFirst().call();
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IOException(e);
                }
            } else {
                List<Future<?>> syncs = new ArrayList<>(work.size());
                for (Callable<Void> c : work) {
                    syncs.add(fsyncPool.submit(c));
                }
                for (Future<?> f : syncs) {
                    try {
                        f.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted during fsync", e);
                    } catch (ExecutionException e) {
                        throw new IOException("fsync failed", e.getCause());
                    }
                }
            }
        }
        for (Replica r : groupArray) {
            r.hardState.markFlushed();
        }
        for (Replica r : dirty) {
            r.log.markFlushed();
            r.raft.onPersisted(r.log.lastIndex());
        }
    }

    private void handleKey(SelectionKey k) {
        Object a = k.attachment();
        if (a == server) {
            accept();
        } else if (a instanceof ClientConn c) {
            c.ready();
        } else if (a instanceof PeerLink l) {
            l.ready();
        }
    }

    private void accept() {
        while (true) {
            SocketChannel s;
            try {
                s = server.accept();
                if (s == null) {
                    return;
                }
                s.configureBlocking(false);
                s.setOption(StandardSocketOptions.TCP_NODELAY, true);
                SelectionKey key = s.register(selector, SelectionKey.OP_READ);
                ClientConn c = new ClientConn(s, key, frames);
                key.attach(c);
                conns.add(c);
            } catch (IOException e) {
                Log.warn("accept failed: " + e);
                return;
            }
        }
    }

    private void shutdownInternal() {
        for (ClientConn c : new ArrayList<>(conns)) {
            c.close();
        }
        for (PeerLink l : links.values()) {
            l.close();
        }
        for (Replica r : groupArray) {
            r.close();
        }
        fsyncPool.shutdownNow();
        try {
            server.close();
            selector.close();
        } catch (IOException e) {
            // ignore
        }
        Log.info("broker " + cfg.nodeId() + " stopped");
    }

    /** Stop serving: flush and close everything, then return. */
    @Override
    public void close() {
        running = false;
        selector.wakeup();
        if (thread != null && thread != Thread.currentThread()) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Run {@code f} on the event loop and wait for its result (tests, tooling). */
    public <T> T call(Function<Broker, T> f) {
        CompletableFuture<T> fut = new CompletableFuture<>();
        tasks.add(() -> {
            try {
                fut.complete(f.apply(this));
            } catch (RuntimeException e) {
                fut.completeExceptionally(e);
            }
        });
        selector.wakeup();
        try {
            return fut.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
    }

    // -----------------------------------------------------------------------
    // Raft transport
    // -----------------------------------------------------------------------

    /**
     * Messages that promise something durable (an appended entry, a vote, a
     * candidacy) wait for this pass's group commit; the rest go out at once.
     */
    void sendRaft(int to, RaftMessage m) {
        boolean promise = switch (m) {
            case RaftMessage.AppendResponse r -> true;
            case RaftMessage.VoteRequest v -> !v.preVote();
            case RaftMessage.VoteResponse v -> !v.preVote();
            case RaftMessage.AppendRequest a -> false;
        };
        if (promise) {
            deferred.add(new Deferred(to, m));
        } else {
            sendNow(to, m);
        }
    }

    private void sendNow(int to, RaftMessage m) {
        PeerLink link = links.get(to);
        if (link == null || blocked.contains(to)) {
            return;
        }
        Writer w = new Writer(64);
        w.i32(0).i16(ApiKey.RAFT).i32(0);
        RaftCodec.encode(m, w);
        ByteBuffer frame = w.toBuffer();
        frame.putInt(0, frame.remaining() - 4);
        link.send(frame);
    }

    // -----------------------------------------------------------------------
    // requests
    // -----------------------------------------------------------------------

    void respond(ClientConn conn, int corr, Consumer<Writer> body) {
        if (conn.isClosed()) {
            return;
        }
        Response r = new Response(corr);
        body.accept(r.w());
        conn.send(r.finish());
    }

    private void handleFrame(ClientConn conn, ByteBuffer frame) {
        Reader in = new Reader(frame);
        short api = in.i16();
        int corr = in.i32();
        long now = now();
        switch (api) {
            case ApiKey.RAFT -> {
                RaftMessage m = RaftCodec.decode(in);
                Replica r = groups.get(m.group());
                if (r != null && !blocked.contains(m.from())) {
                    r.raft.receive(m, now);
                }
            }
            case ApiKey.METADATA -> metadataRequest(conn, corr, MetadataRequest.decode(in));
            case ApiKey.CREATE_TOPIC -> createTopic(conn, corr, CreateTopicRequest.decode(in));
            case ApiKey.PRODUCE -> produce(conn, corr, ProduceRequest.decode(in));
            case ApiKey.FETCH -> fetch(conn, corr, FetchRequest.decode(in), now);
            case ApiKey.JOIN_GROUP -> {
                JoinGroupRequest req = JoinGroupRequest.decode(in);
                if (coordinatorReady()) {
                    coordinator.join(conn, corr, req, now);
                } else {
                    respond(conn, corr, new JoinGroupResponse(Errors.NOT_COORDINATOR, offsets.raft.leaderId(), -1,
                            req.memberId(), List.of())::encode);
                }
            }
            case ApiKey.HEARTBEAT -> {
                HeartbeatRequest req = HeartbeatRequest.decode(in);
                simple(conn, corr, coordinatorReady() ? coordinator.heartbeat(req, now) : Errors.NOT_COORDINATOR,
                        offsets.raft.leaderId());
            }
            case ApiKey.LEAVE_GROUP -> {
                LeaveGroupRequest req = LeaveGroupRequest.decode(in);
                simple(conn, corr, coordinatorReady() ? coordinator.leave(req, now) : Errors.NOT_COORDINATOR,
                        offsets.raft.leaderId());
            }
            case ApiKey.OFFSET_COMMIT -> offsetCommit(conn, corr, OffsetCommitRequest.decode(in));
            case ApiKey.OFFSET_FETCH -> offsetFetch(conn, corr, OffsetFetchRequest.decode(in));
            case ApiKey.STATUS -> respond(conn, corr, status()::encode);
            case ApiKey.CHAOS -> {
                int[] ids = ChaosRequest.decode(in).blockedPeers();
                blocked = Set.copyOf(Arrays.stream(ids).boxed().toList());
                Log.warn("broker " + cfg.nodeId() + ": chaos, now dropping traffic with " + blocked);
                simple(conn, corr, Errors.NONE, -1);
            }
            default -> throw new Wire.ProtocolException("unknown api key " + api);
        }
    }

    private void simple(ClientConn conn, int corr, Errors e, int hint) {
        respond(conn, corr, new SimpleResponse(e, hint)::encode);
    }

    private boolean coordinatorReady() {
        return coordinator.active() && offsets.raft.isReady();
    }

    StatusResponse status() {
        List<GroupStatus> out = new ArrayList<>();
        for (Replica r : groupArray) {
            out.add(r.status());
        }
        return new StatusResponse(cfg.nodeId(), blocked.stream().mapToInt(Integer::intValue).sorted().toArray(), out);
    }

    private void metadataRequest(ClientConn conn, int corr, MetadataRequest req) {
        List<BrokerInfo> brokers = new ArrayList<>();
        for (Map.Entry<Integer, InetSocketAddress> e : cfg.cluster().entrySet()) {
            brokers.add(new BrokerInfo(e.getKey(), e.getValue().getHostString(), e.getValue().getPort()));
        }
        List<String> names = req.topics().isEmpty() ? new ArrayList<>(metadata.topics().keySet()) : req.topics();
        List<TopicInfo> topics = new ArrayList<>();
        for (String name : names) {
            MetadataReplica.TopicMeta t = metadata.topics().get(name);
            if (t == null) {
                topics.add(new TopicInfo(name, Errors.UNKNOWN_TOPIC_OR_PARTITION, List.of()));
                continue;
            }
            List<PartitionInfo> parts = new ArrayList<>();
            for (int p = 0; p < t.partitions(); p++) {
                PartitionReplica r = partitions.get(new TopicPartition(name, p));
                parts.add(r == null ? new PartitionInfo(p, -1, 0)
                        : new PartitionInfo(p, r.raft.leaderId(), r.highWatermark()));
            }
            topics.add(new TopicInfo(name, Errors.NONE, parts));
        }
        MetadataResponse resp = new MetadataResponse(cfg.nodeId(), brokers, metadata.raft.leaderId(),
                offsets.raft.leaderId(), topics);
        respond(conn, corr, resp::encode);
    }

    private void createTopic(ClientConn conn, int corr, CreateTopicRequest req) {
        RaftNode raft = metadata.raft;
        if (!raft.isLeader()) {
            simple(conn, corr, Errors.NOT_LEADER, raft.leaderId());
            return;
        }
        if (!raft.isReady()) {
            simple(conn, corr, Errors.LEADER_NOT_AVAILABLE, cfg.nodeId());
            return;
        }
        if (req.name() == null || !TOPIC_NAME.matcher(req.name()).matches() || req.name().startsWith("__")) {
            simple(conn, corr, Errors.INVALID_TOPIC, -1);
            return;
        }
        if (req.partitions() < 1 || req.partitions() > 10_000) {
            simple(conn, corr, Errors.INVALID_REQUEST, -1);
            return;
        }
        if (metadata.topics().containsKey(req.name())) {
            simple(conn, corr, Errors.TOPIC_ALREADY_EXISTS, -1);
            return;
        }
        try {
            long idx = metadata.propose(MetadataReplica.createTopic(req.name(), req.partitions()));
            metadata.whenCommitted(idx, () -> simple(conn, corr, Errors.NONE, cfg.nodeId()),
                    e -> simple(conn, corr, e, metadata.raft.leaderId()));
        } catch (NotLeaderException e) {
            simple(conn, corr, Errors.NOT_LEADER, e.leaderHint);
        }
    }

    private void produce(ClientConn conn, int corr, ProduceRequest req) {
        int n = req.partitions().size();
        ProduceResult[] results = new ProduceResult[n];
        int[] remaining = {n};
        boolean[] done = {false};
        Runnable finish = () -> {
            if (!done[0] && remaining[0] == 0) {
                done[0] = true;
                respond(conn, corr, new ProduceResponse(Arrays.asList(results))::encode);
            }
        };
        for (int i = 0; i < n; i++) {
            ProducePartition pp = req.partitions().get(i);
            PartitionReplica r = partitions.get(new TopicPartition(pp.topic(), pp.partition()));
            if (r == null) {
                results[i] = new ProduceResult(pp.topic(), pp.partition(), Errors.UNKNOWN_TOPIC_OR_PARTITION, -1,
                        new long[0]);
                remaining[0]--;
                continue;
            }
            if (!r.raft.isLeader()) {
                results[i] = new ProduceResult(pp.topic(), pp.partition(), Errors.NOT_LEADER, r.raft.leaderId(),
                        new long[0]);
                remaining[0]--;
                continue;
            }
            long[] offs = new long[pp.records().size()];
            long max = -1;
            Errors err = Errors.NONE;
            for (int k = 0; k < offs.length && err == Errors.NONE; k++) {
                ProduceRecord rec = pp.records().get(k);
                if (rec.producerId() >= 0) {
                    Integer last = r.lastSequence(rec.producerId());
                    if (last != null && rec.sequence() <= last) {
                        offs[k] = r.offsetOf(rec.producerId(), rec.sequence()); // a retry: already in the log
                        max = Math.max(max, offs[k]);
                        continue;
                    }
                    if (last != null && rec.sequence() != last + 1) {
                        err = Errors.OUT_OF_ORDER_SEQUENCE;
                        break;
                    }
                }
                Record entry = Record.data(rec.timestamp(), rec.producerId(), rec.sequence(), rec.key(), rec.value());
                if (entry.encodedSize() > Record.MAX_SIZE) {
                    err = Errors.INVALID_REQUEST;
                    break;
                }
                try {
                    offs[k] = r.propose(entry);
                    max = Math.max(max, offs[k]);
                } catch (NotLeaderException e) {
                    err = Errors.NOT_LEADER;
                }
            }
            if (err != Errors.NONE) {
                results[i] = new ProduceResult(pp.topic(), pp.partition(), err, r.raft.leaderId(), new long[0]);
                remaining[0]--;
                continue;
            }
            if (max < 0) {
                results[i] = new ProduceResult(pp.topic(), pp.partition(), Errors.NONE, cfg.nodeId(), offs);
                remaining[0]--;
                continue;
            }
            int idx = i;
            r.whenCommitted(max, () -> {
                results[idx] = new ProduceResult(pp.topic(), pp.partition(), Errors.NONE, cfg.nodeId(), offs);
                remaining[0]--;
                finish.run();
            }, e -> {
                results[idx] = new ProduceResult(pp.topic(), pp.partition(), e, r.raft.leaderId(), new long[0]);
                remaining[0]--;
                finish.run();
            });
        }
        if (remaining[0] > 0 && req.timeoutMs() > 0) {
            schedule(req.timeoutMs(), () -> {
                if (done[0]) {
                    return;
                }
                for (int i = 0; i < n; i++) {
                    if (results[i] == null) {
                        ProducePartition pp = req.partitions().get(i);
                        results[i] = new ProduceResult(pp.topic(), pp.partition(), Errors.REQUEST_TIMED_OUT, -1,
                                new long[0]);
                    }
                }
                done[0] = true;
                respond(conn, corr, new ProduceResponse(Arrays.asList(results))::encode);
            });
        }
        finish.run();
    }

    // -----------------------------------------------------------------------
    // fetch (long poll, zero-copy)
    // -----------------------------------------------------------------------

    private void fetch(ClientConn conn, int corr, FetchRequest req, long now) {
        tryFetch(new FetchWait(conn, corr, req, now + Math.max(0, req.maxWaitMs())), false);
    }

    private void tryFetch(FetchWait w, boolean force) {
        if (w.done) {
            return;
        }
        List<FetchResult> results = new ArrayList<>();
        List<Region> regions = new ArrayList<>();
        List<PartitionReplica> involved = new ArrayList<>();
        boolean any = false;
        for (FetchPartition fp : w.req.partitions()) {
            PartitionReplica r = partitions.get(new TopicPartition(fp.topic(), fp.partition()));
            Errors err = Errors.NONE;
            int hint = -1;
            long hw = -1;
            Region region = null;
            if (r == null) {
                err = Errors.UNKNOWN_TOPIC_OR_PARTITION;
            } else if (!r.raft.isLeader()) {
                err = Errors.NOT_LEADER;
                hint = r.raft.leaderId();
            } else if (!r.raft.isReady()) {
                err = Errors.LEADER_NOT_AVAILABLE;
                hint = cfg.nodeId();
            } else {
                involved.add(r);
                hint = cfg.nodeId();
                hw = r.highWatermark();
                if (fp.offset() < 0 || fp.offset() > hw) {
                    err = Errors.OFFSET_OUT_OF_RANGE;
                } else {
                    try {
                        region = r.log.region(fp.offset(), hw, Math.max(1, fp.maxBytes()));
                    } catch (IOException e) {
                        err = Errors.STORAGE_ERROR;
                    }
                }
            }
            any |= err != Errors.NONE || region != null;
            results.add(new FetchResult(fp.topic(), fp.partition(), err, hint, hw, null));
            regions.add(region);
        }
        if (!any && !force && w.req.maxWaitMs() > 0 && now() < w.deadline) {
            if (w.parkedOn.isEmpty()) {
                schedule(Math.max(1, w.deadline - now()), () -> tryFetch(w, true));
            }
            for (PartitionReplica r : involved) {
                if (!w.parkedOn.contains(r)) {
                    w.parkedOn.add(r);
                    fetchWaits.computeIfAbsent(r, k -> new ArrayList<>()).add(w);
                }
            }
            return;
        }
        w.done = true;
        for (PartitionReplica r : w.parkedOn) {
            List<FetchWait> l = fetchWaits.get(r);
            if (l != null) {
                l.remove(w);
                if (l.isEmpty()) {
                    fetchWaits.remove(r);
                }
            }
        }
        if (w.conn.isClosed()) {
            return;
        }
        Response resp = new Response(w.corr);
        resp.w().i32(results.size());
        for (int i = 0; i < results.size(); i++) {
            Region region = regions.get(i);
            results.get(i).encodeHeader(resp.w(), region == null ? 0 : region.length());
            if (region != null) {
                resp.region(region);
            }
        }
        w.conn.send(resp.finish());
    }

    /** The partition's high watermark moved (or it lost leadership): retry parked fetches. */
    void wakeFetchers(PartitionReplica r) {
        List<FetchWait> l = fetchWaits.remove(r);
        if (l == null) {
            return;
        }
        for (FetchWait w : new ArrayList<>(l)) {
            w.parkedOn.remove(r);
            tryFetch(w, false);
        }
    }

    // -----------------------------------------------------------------------
    // offsets
    // -----------------------------------------------------------------------

    private void offsetCommit(ClientConn conn, int corr, OffsetCommitRequest req) {
        if (!coordinatorReady()) {
            simple(conn, corr, Errors.NOT_COORDINATOR, offsets.raft.leaderId());
            return;
        }
        Errors check = coordinator.checkCommit(req.group(), req.memberId(), req.generation());
        if (check != Errors.NONE) {
            simple(conn, corr, check, cfg.nodeId());
            return;
        }
        if (req.offsets().isEmpty()) {
            simple(conn, corr, Errors.NONE, cfg.nodeId());
            return;
        }
        try {
            long idx = offsets.propose(OffsetsReplica.commit(req.group(), req.offsets()));
            offsets.whenCommitted(idx, () -> simple(conn, corr, Errors.NONE, cfg.nodeId()),
                    e -> simple(conn, corr, Errors.NOT_COORDINATOR, offsets.raft.leaderId()));
        } catch (NotLeaderException e) {
            simple(conn, corr, Errors.NOT_COORDINATOR, e.leaderHint);
        }
    }

    private void offsetFetch(ClientConn conn, int corr, OffsetFetchRequest req) {
        if (!coordinatorReady()) {
            respond(conn, corr, new OffsetFetchResponse(Errors.NOT_COORDINATOR, offsets.raft.leaderId(),
                    List.of())::encode);
            return;
        }
        List<PartitionOffset> out = new ArrayList<>();
        for (TopicPartition tp : req.partitions()) {
            out.add(new PartitionOffset(tp.topic(), tp.partition(), offsets.offset(req.group(), tp)));
        }
        respond(conn, corr, new OffsetFetchResponse(Errors.NONE, cfg.nodeId(), out)::encode);
    }

    /** group → member → assignment, as the coordinator (if this broker is it) sees it. */
    public Map<String, Map<String, List<TopicPartition>>> describeGroups() {
        return call(b -> b.coordinator.describe());
    }
}
