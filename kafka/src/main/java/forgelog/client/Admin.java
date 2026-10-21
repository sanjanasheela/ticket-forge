package forgelog.client;

import forgelog.protocol.ApiKey;
import forgelog.protocol.Errors;
import forgelog.protocol.Messages.ChaosRequest;
import forgelog.protocol.Messages.CreateTopicRequest;
import forgelog.protocol.Messages.MetadataResponse;
import forgelog.protocol.Messages.PartitionInfo;
import forgelog.protocol.Messages.SimpleResponse;
import forgelog.protocol.Messages.StatusResponse;
import forgelog.protocol.Messages.TopicInfo;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Cluster administration: topics, per-broker Raft status, fault injection. */
public final class Admin implements Closeable {
    private final ClusterClient cluster;

    public Admin(String bootstrapServers) {
        this.cluster = new ClusterClient(bootstrapServers);
    }

    public ClusterClient cluster() {
        return cluster;
    }

    /**
     * Create a topic and wait until every partition has elected a leader.
     *
     * @throws ForgeLogException with {@link Errors#TOPIC_ALREADY_EXISTS} if it exists
     */
    public void createTopic(String name, int partitions, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        boolean created = false;
        boolean ambiguous = false; // a request may have succeeded without us seeing the answer
        while (!created) {
            checkDeadline(deadline, "create topic " + name);
            try {
                int target = cluster.controller();
                if (target < 0) {
                    cluster.refreshMetadata();
                    target = cluster.controller();
                }
                SimpleResponse r = SimpleResponse.decode(target >= 0
                        ? cluster.call(target, ApiKey.CREATE_TOPIC, new CreateTopicRequest(name, partitions)::encode,
                                10_000)
                        : cluster.callAny(ApiKey.CREATE_TOPIC, new CreateTopicRequest(name, partitions)::encode,
                                10_000));
                switch (r.error()) {
                    case NONE -> created = true;
                    case TOPIC_ALREADY_EXISTS -> {
                        if (!ambiguous) {
                            throw new ForgeLogException(r.error(), "topic " + name + " already exists");
                        }
                        created = true;
                    }
                    case NOT_LEADER, LEADER_NOT_AVAILABLE -> {
                        cluster.setController(r.error() == Errors.NOT_LEADER ? r.leaderHint() : -1);
                        pause();
                    }
                    default -> throw new ForgeLogException(r.error(), "create topic " + name + ": " + r.error());
                }
            } catch (IOException e) {
                ambiguous = true;
                cluster.setController(-1);
                pause();
            }
        }
        // Every broker opens the partitions as the creation commits; wait for leaders.
        while (true) {
            checkDeadline(deadline, "leaders for " + name);
            try {
                MetadataResponse m = cluster.refreshMetadata(List.of(name));
                TopicInfo t = m.topics().getFirst();
                if (t.error() == Errors.NONE && t.partitions().size() == partitions
                        && t.partitions().stream().allMatch(p -> p.leader() >= 0)) {
                    return;
                }
            } catch (IOException e) {
                // try another broker
            }
            pause();
        }
    }

    public void createTopic(String name, int partitions) {
        createTopic(name, partitions, Duration.ofSeconds(30));
    }

    private static void checkDeadline(long deadline, String what) {
        if (System.nanoTime() > deadline) {
            throw new ForgeLogException(Errors.REQUEST_TIMED_OUT, "timed out: " + what);
        }
    }

    private static void pause() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ForgeLogException("interrupted");
        }
    }

    /** topic → partition → leader broker (-1 = none). */
    public Map<String, int[]> describeTopics() throws IOException {
        Map<String, int[]> out = new TreeMap<>();
        for (TopicInfo t : cluster.refreshMetadata().topics()) {
            int[] l = new int[t.partitions().size()];
            for (PartitionInfo p : t.partitions()) {
                l[p.partition()] = p.leader();
            }
            out.put(t.name(), l);
        }
        return out;
    }

    public MetadataResponse metadata() throws IOException {
        return cluster.refreshMetadata();
    }

    public StatusResponse status(int brokerId) throws IOException {
        if (cluster.address(brokerId) == null) {
            cluster.refreshMetadata();
        }
        return StatusResponse.decode(cluster.call(brokerId, ApiKey.STATUS, w -> {}, 5_000));
    }

    /** STATUS from every reachable broker. */
    public List<StatusResponse> statusAll() throws IOException {
        cluster.refreshMetadata();
        List<StatusResponse> out = new ArrayList<>();
        for (int id : new TreeMap<>(cluster.brokers()).keySet()) {
            try {
                out.add(status(id));
            } catch (IOException e) {
                // unreachable: leave it out
            }
        }
        return out;
    }

    /** Make {@code brokerId} drop all Raft traffic to and from {@code blocked} (empty heals it). */
    public void chaos(int brokerId, int... blocked) throws IOException {
        if (cluster.address(brokerId) == null) {
            cluster.refreshMetadata();
        }
        SimpleResponse r = SimpleResponse.decode(cluster.call(brokerId, ApiKey.CHAOS,
                new ChaosRequest(blocked)::encode, 5_000));
        if (r.error() != Errors.NONE) {
            throw new ForgeLogException(r.error(), "chaos: " + r.error());
        }
    }

    @Override
    public void close() {
        cluster.close();
    }
}
