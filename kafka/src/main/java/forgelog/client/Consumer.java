package forgelog.client;

import forgelog.log.CorruptRecordException;
import forgelog.log.Record;
import forgelog.protocol.ApiKey;
import forgelog.protocol.Errors;
import forgelog.protocol.Messages.FetchPartition;
import forgelog.protocol.Messages.FetchRequest;
import forgelog.protocol.Messages.FetchResponse;
import forgelog.protocol.Messages.FetchResult;
import forgelog.protocol.Messages.HeartbeatRequest;
import forgelog.protocol.Messages.JoinGroupRequest;
import forgelog.protocol.Messages.JoinGroupResponse;
import forgelog.protocol.Messages.LeaveGroupRequest;
import forgelog.protocol.Messages.OffsetCommitRequest;
import forgelog.protocol.Messages.OffsetFetchRequest;
import forgelog.protocol.Messages.OffsetFetchResponse;
import forgelog.protocol.Messages.PartitionOffset;
import forgelog.protocol.Messages.SimpleResponse;
import forgelog.protocol.TopicPartition;
import forgelog.util.Log;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A consumer-group member.
 *
 * <p>{@link #poll} joins (or rejoins) the group when needed, resumes each
 * assigned partition from its committed offset, and long-polls the
 * partitions' leaders. {@link #commitSync} stores the current positions in
 * the replicated offsets log. Delivery is at-least-once: records after the
 * last commit are redelivered after a crash or a rebalance. Commits are
 * fenced by generation, so a member that lost a partition can't move its
 * offset any more.
 *
 * <p>Heartbeats run on their own thread and connection; a missed session
 * timeout (e.g. a crashed process) makes the coordinator rebalance its
 * partitions onto the remaining members.
 *
 * <p>Not thread-safe (like Kafka's consumer): use it from one thread.
 */
public final class Consumer implements Closeable {
    public record Config(int sessionTimeoutMs, int maxPollRecords, int fetchMaxBytes) {
        public static final Config DEFAULT = new Config(10_000, 1_000, 1 << 20);
    }

    public record ConsumerRecord(String topic, int partition, long offset, long timestamp, byte[] key, byte[] value) {
        public String keyString() {
            return key == null ? null : new String(key, StandardCharsets.UTF_8);
        }

        public String valueString() {
            return value == null ? null : new String(value, StandardCharsets.UTF_8);
        }
    }

    private final ClusterClient cluster;
    private final String group;
    private final List<String> topics;
    private final Config cfg;
    private volatile String memberId = "";
    private volatile int generation = -1;
    private volatile boolean rejoinNeeded = true;
    private volatile boolean closed;
    private List<TopicPartition> assignment = List.of();
    private final Map<TopicPartition, Long> positions = new LinkedHashMap<>();
    private int nextLeaderIndex;
    private final Thread heartbeats;

    public Consumer(String bootstrapServers, String group, Collection<String> topics) {
        this(bootstrapServers, group, topics, Config.DEFAULT);
    }

    public Consumer(String bootstrapServers, String group, Collection<String> topics, Config cfg) {
        this.cluster = new ClusterClient(bootstrapServers);
        this.group = group;
        this.topics = List.copyOf(topics);
        this.cfg = cfg;
        this.heartbeats = new Thread(this::runHeartbeats, "forgelog-consumer-heartbeat");
        heartbeats.setDaemon(true);
        heartbeats.start();
    }

    public List<TopicPartition> assignment() {
        return assignment;
    }

    public String memberId() {
        return memberId;
    }

    public int generation() {
        return generation;
    }

    /** Next offset to be returned for each assigned partition. */
    public Map<TopicPartition, Long> positions() {
        return new LinkedHashMap<>(positions);
    }

    // -----------------------------------------------------------------------
    // group membership
    // -----------------------------------------------------------------------

    private int coordinator() throws IOException {
        int c = cluster.coordinator();
        if (c < 0) {
            cluster.refreshMetadata();
            c = cluster.coordinator();
        }
        if (c < 0) {
            throw new IOException("no group coordinator yet");
        }
        return c;
    }

    private void join(long deadline) {
        while (!closed) {
            if (System.nanoTime() > deadline) {
                return; // poll() returns nothing; the next poll keeps trying
            }
            try {
                int coord = coordinator();
                JoinGroupResponse r = JoinGroupResponse.decode(cluster.call(coord, ApiKey.JOIN_GROUP,
                        new JoinGroupRequest(group, memberId, cfg.sessionTimeoutMs(), topics)::encode,
                        cfg.sessionTimeoutMs() + 10_000));
                switch (r.error()) {
                    case NONE -> {
                        memberId = r.memberId();
                        generation = r.generation();
                        assignment = r.assignment();
                        // Its own budget: the join itself may have outlasted the poll timeout.
                        if (resumeFromCommitted(System.nanoTime() + Duration.ofSeconds(10).toNanos())) {
                            rejoinNeeded = false;
                        }
                        Log.debug("consumer " + memberId + ": generation " + generation + ", assigned " + assignment);
                        return;
                    }
                    case NOT_COORDINATOR -> {
                        cluster.setCoordinator(r.coordinatorHint());
                        memberId = ""; // a new coordinator starts with no members
                    }
                    case UNKNOWN_MEMBER_ID -> memberId = "";
                    default -> {
                        // REBALANCE_IN_PROGRESS etc.: just rejoin
                    }
                }
            } catch (IOException e) {
                cluster.setCoordinator(-1);
            }
            sleep(50);
        }
    }

    /** Positions for the new assignment from the committed offsets; {@code false} if out of time. */
    private boolean resumeFromCommitted(long deadline) {
        positions.clear();
        if (assignment.isEmpty()) {
            return true;
        }
        do {
            try {
                OffsetFetchResponse r = OffsetFetchResponse.decode(cluster.call(coordinator(), ApiKey.OFFSET_FETCH,
                        new OffsetFetchRequest(group, assignment)::encode, 10_000));
                if (r.error() == Errors.NONE) {
                    for (PartitionOffset o : r.offsets()) {
                        positions.put(new TopicPartition(o.topic(), o.partition()), Math.max(0, o.offset()));
                    }
                    return true;
                }
                cluster.setCoordinator(r.coordinatorHint());
            } catch (IOException e) {
                cluster.setCoordinator(-1);
            }
            sleep(50);
        } while (!closed && System.nanoTime() < deadline);
        return false;
    }

    private void runHeartbeats() {
        BrokerConnection conn = null;
        int connTo = -1;
        while (!closed) {
            sleep(Math.max(50, cfg.sessionTimeoutMs() / 4));
            if (closed || rejoinNeeded || generation < 0) {
                continue;
            }
            try {
                int coord = coordinator();
                if (conn == null || connTo != coord || conn.isClosed()) {
                    if (conn != null) {
                        conn.close();
                    }
                    InetSocketAddress a = cluster.address(coord);
                    if (a == null) {
                        cluster.refreshMetadata();
                        a = cluster.address(coord);
                    }
                    conn = new BrokerConnection(a, ClusterClient.CONNECT_TIMEOUT_MS);
                    connTo = coord;
                }
                int gen = generation;
                String member = memberId;
                SimpleResponse r = SimpleResponse.decode(conn.call(ApiKey.HEARTBEAT,
                        new HeartbeatRequest(group, member, gen)::encode, 5_000));
                if (gen != generation || rejoinNeeded) {
                    continue; // answer about a generation we no longer have (a join finished meanwhile)
                }
                switch (r.error()) {
                    case NONE -> {}
                    case NOT_COORDINATOR -> {
                        cluster.setCoordinator(r.leaderHint());
                        memberId = "";
                        rejoinNeeded = true;
                    }
                    case UNKNOWN_MEMBER_ID -> {
                        memberId = "";
                        rejoinNeeded = true;
                    }
                    default -> rejoinNeeded = true; // REBALANCE_IN_PROGRESS, ILLEGAL_GENERATION
                }
            } catch (IOException | RuntimeException e) {
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
                cluster.setCoordinator(-1);
            }
        }
        if (conn != null) {
            conn.close();
        }
    }

    // -----------------------------------------------------------------------
    // consuming
    // -----------------------------------------------------------------------

    /** Records from the assigned partitions, waiting up to {@code timeout} for some to arrive. */
    public List<ConsumerRecord> poll(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        List<ConsumerRecord> out = new ArrayList<>();
        while (!closed) {
            if (rejoinNeeded) {
                join(deadline);
                if (rejoinNeeded) {
                    return out;
                }
            }
            if (assignment.isEmpty()) {
                long left = (deadline - System.nanoTime()) / 1_000_000;
                if (left <= 0) {
                    return out;
                }
                sleep(Math.min(left, 100));
                continue;
            }
            Map<Integer, List<TopicPartition>> byLeader = new LinkedHashMap<>();
            boolean unknown = false;
            for (TopicPartition tp : assignment) {
                int l = cluster.leader(tp);
                if (l < 0) {
                    unknown = true;
                } else {
                    byLeader.computeIfAbsent(l, k -> new ArrayList<>()).add(tp);
                }
            }
            if (unknown) {
                try {
                    cluster.refreshMetadata();
                } catch (IOException e) {
                    // retry below
                }
            }
            if (!byLeader.isEmpty()) {
                List<Integer> leaders = new ArrayList<>(byLeader.keySet());
                long left = Math.max(0, (deadline - System.nanoTime()) / 1_000_000);
                // One leader: block on it. Several: short polls so none starves the others.
                int wait = (int) (leaders.size() == 1 ? Math.min(left, 1_000) : Math.min(left, 50));
                for (int i = 0; i < leaders.size() && out.size() < cfg.maxPollRecords(); i++) {
                    int leader = leaders.get((nextLeaderIndex + i) % leaders.size());
                    fetchFrom(leader, byLeader.get(leader), wait, out);
                    if (rejoinNeeded) {
                        break;
                    }
                }
                nextLeaderIndex++;
            } else {
                sleep(20);
            }
            if (!out.isEmpty() || System.nanoTime() >= deadline) {
                return out;
            }
        }
        return out;
    }

    private void fetchFrom(int leader, List<TopicPartition> parts, int waitMs, List<ConsumerRecord> out) {
        List<FetchPartition> req = new ArrayList<>();
        for (TopicPartition tp : parts) {
            req.add(new FetchPartition(tp.topic(), tp.partition(), positions.getOrDefault(tp, 0L), cfg.fetchMaxBytes()));
        }
        FetchResponse resp;
        try {
            resp = FetchResponse.decode(cluster.call(leader, ApiKey.FETCH, new FetchRequest(waitMs, req)::encode,
                    waitMs + 10_000));
        } catch (IOException | RuntimeException e) {
            for (TopicPartition tp : parts) {
                cluster.setLeader(tp, -1);
            }
            sleep(20);
            return;
        }
        for (FetchResult r : resp.results()) {
            TopicPartition tp = new TopicPartition(r.topic(), r.partition());
            if (!positions.containsKey(tp)) {
                continue; // revoked meanwhile
            }
            switch (r.error()) {
                case NONE -> decode(tp, r.records(), out);
                case NOT_LEADER -> cluster.setLeader(tp, r.leaderHint());
                case OFFSET_OUT_OF_RANGE -> {
                    Log.warn("consumer: offset " + positions.get(tp) + " out of range for " + tp + " (hw "
                            + r.highWatermark() + "), resetting to the start");
                    positions.put(tp, 0L);
                }
                default -> cluster.setLeader(tp, -1);
            }
        }
    }

    private void decode(TopicPartition tp, ByteBuffer raw, List<ConsumerRecord> out) {
        long pos = positions.get(tp);
        try {
            while (raw.hasRemaining()) {
                Record r = Record.decode(raw);
                if (r == null) {
                    break;
                }
                if (r.offset < pos) {
                    continue;
                }
                if (r.isData()) {
                    out.add(new ConsumerRecord(tp.topic(), tp.partition(), r.offset, r.timestamp, r.key, r.value));
                }
                pos = r.offset + 1; // control records are skipped, but consumed
            }
        } catch (CorruptRecordException e) {
            throw new ForgeLogException("corrupt record from " + tp + ": " + e.getMessage());
        }
        positions.put(tp, pos);
    }

    // -----------------------------------------------------------------------
    // offsets
    // -----------------------------------------------------------------------

    /**
     * Commit the current positions of every assigned partition.
     *
     * @throws ForgeLogException.CommitFailedException if the group rebalanced meanwhile
     */
    public void commitSync() {
        commitSync(positions());
    }

    public void commitSync(Map<TopicPartition, Long> offsets) {
        if (offsets.isEmpty()) {
            return;
        }
        List<PartitionOffset> list = new ArrayList<>();
        for (Map.Entry<TopicPartition, Long> e : offsets.entrySet()) {
            list.add(new PartitionOffset(e.getKey().topic(), e.getKey().partition(), e.getValue()));
        }
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            try {
                SimpleResponse r = SimpleResponse.decode(cluster.call(coordinator(), ApiKey.OFFSET_COMMIT,
                        new OffsetCommitRequest(group, memberId, generation, list)::encode, 15_000));
                switch (r.error()) {
                    case NONE -> {
                        return;
                    }
                    case NOT_COORDINATOR -> {
                        cluster.setCoordinator(r.leaderHint());
                        memberId = "";
                        rejoinNeeded = true;
                        throw new ForgeLogException.CommitFailedException(r.error());
                    }
                    case ILLEGAL_GENERATION, UNKNOWN_MEMBER_ID, REBALANCE_IN_PROGRESS -> {
                        rejoinNeeded = true;
                        throw new ForgeLogException.CommitFailedException(r.error());
                    }
                    default -> throw new ForgeLogException(r.error(), "offset commit failed: " + r.error());
                }
            } catch (IOException e) {
                cluster.setCoordinator(-1);
                sleep(50);
            }
        }
        throw new ForgeLogException(Errors.REQUEST_TIMED_OUT, "offset commit timed out");
    }

    /** Committed offsets for the given partitions (-1 = none). */
    public Map<TopicPartition, Long> committed(Collection<TopicPartition> parts) throws IOException {
        OffsetFetchResponse r = OffsetFetchResponse.decode(cluster.call(coordinator(), ApiKey.OFFSET_FETCH,
                new OffsetFetchRequest(group, List.copyOf(parts))::encode, 10_000));
        if (r.error() != Errors.NONE) {
            throw new ForgeLogException(r.error(), "offset fetch failed: " + r.error());
        }
        Map<TopicPartition, Long> out = new HashMap<>();
        for (PartitionOffset o : r.offsets()) {
            out.put(new TopicPartition(o.topic(), o.partition()), o.offset());
        }
        return out;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Leave the group (its partitions are reassigned right away) and disconnect. */
    @Override
    public void close() {
        closed = true;
        if (generation >= 0 && !memberId.isEmpty()) {
            try {
                cluster.call(coordinator(), ApiKey.LEAVE_GROUP, new LeaveGroupRequest(group, memberId)::encode, 3_000);
            } catch (IOException | RuntimeException e) {
                // the session timeout will catch it
            }
        }
        heartbeats.interrupt();
        cluster.close();
    }

    /** Stop without leaving the group, as if the process died (tests). */
    public void abandon() {
        closed = true;
        heartbeats.interrupt();
        cluster.close();
    }
}
