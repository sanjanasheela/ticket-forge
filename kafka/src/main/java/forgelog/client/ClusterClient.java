package forgelog.client;

import forgelog.protocol.ApiKey;
import forgelog.protocol.Errors;
import forgelog.protocol.Messages.BrokerInfo;
import forgelog.protocol.Messages.MetadataRequest;
import forgelog.protocol.Messages.MetadataResponse;
import forgelog.protocol.Messages.PartitionInfo;
import forgelog.protocol.Messages.TopicInfo;
import forgelog.protocol.TopicPartition;
import forgelog.protocol.Wire.Reader;
import forgelog.protocol.Wire.Writer;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * What every client needs: connections to brokers, and a cache of the
 * cluster layout (brokers, partition leaders, controller, coordinator)
 * refreshed on demand and corrected by the leader hints in error responses.
 * Thread-safe.
 */
public final class ClusterClient implements Closeable {
    static final int CONNECT_TIMEOUT_MS = 3_000;

    private final List<InetSocketAddress> bootstrap;
    private final Map<Integer, InetSocketAddress> brokers = new ConcurrentHashMap<>();
    private final Map<Integer, BrokerConnection> conns = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Integer> leaders = new ConcurrentHashMap<>();
    private final Map<String, Integer> partitionCounts = new ConcurrentHashMap<>();
    private volatile int controller = -1;
    private volatile int coordinator = -1;
    private volatile boolean closed;

    public ClusterClient(String bootstrapServers) {
        this.bootstrap = parse(bootstrapServers);
    }

    public static List<InetSocketAddress> parse(String servers) {
        List<InetSocketAddress> out = new ArrayList<>();
        for (String s : servers.split(",")) {
            String t = s.trim();
            if (t.isEmpty()) {
                continue;
            }
            int colon = t.lastIndexOf(':');
            if (colon <= 0) {
                throw new IllegalArgumentException("bad server '" + t + "' (expected host:port)");
            }
            out.add(new InetSocketAddress(t.substring(0, colon), Integer.parseInt(t.substring(colon + 1))));
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("no bootstrap servers");
        }
        return out;
    }

    private BrokerConnection connection(int brokerId) throws IOException {
        if (closed) {
            throw new IOException("client closed");
        }
        BrokerConnection c = conns.get(brokerId);
        if (c != null && !c.isClosed()) {
            return c;
        }
        InetSocketAddress addr = brokers.get(brokerId);
        if (addr == null) {
            throw new IOException("unknown broker " + brokerId);
        }
        c = new BrokerConnection(addr, CONNECT_TIMEOUT_MS);
        BrokerConnection prev = conns.put(brokerId, c);
        if (prev != null) {
            prev.close();
        }
        return c;
    }

    /** One request to one broker. On I/O failure the connection is dropped (the next call reconnects). */
    public Reader call(int brokerId, short api, Consumer<Writer> body, int timeoutMs) throws IOException {
        BrokerConnection c = connection(brokerId);
        try {
            return c.call(api, body, timeoutMs);
        } catch (IOException e) {
            c.close();
            conns.remove(brokerId, c);
            throw e;
        }
    }

    /** One request to whichever broker answers first (known brokers, then the bootstrap list). */
    public Reader callAny(short api, Consumer<Writer> body, int timeoutMs) throws IOException {
        IOException last = null;
        List<Integer> ids = new ArrayList<>(brokers.keySet());
        Collections.shuffle(ids);
        for (int id : ids) {
            try {
                return call(id, api, body, timeoutMs);
            } catch (IOException e) {
                last = e;
            }
        }
        for (InetSocketAddress a : bootstrap) {
            try (BrokerConnection c = new BrokerConnection(a, CONNECT_TIMEOUT_MS)) {
                return c.call(api, body, timeoutMs);
            } catch (IOException e) {
                last = e;
            }
        }
        throw last != null ? last : new IOException("no broker reachable");
    }

    /** Fetch fresh metadata (for {@code topics}, or all topics if empty) and update the cache. */
    public MetadataResponse refreshMetadata(List<String> topics) throws IOException {
        MetadataResponse m = MetadataResponse.decode(callAny(ApiKey.METADATA,
                new MetadataRequest(topics)::encode, CONNECT_TIMEOUT_MS));
        for (BrokerInfo b : m.brokers()) {
            brokers.put(b.id(), new InetSocketAddress(b.host(), b.port()));
        }
        controller = m.controller();
        coordinator = m.coordinator();
        for (TopicInfo t : m.topics()) {
            if (t.error() != Errors.NONE) {
                continue;
            }
            partitionCounts.put(t.name(), t.partitions().size());
            for (PartitionInfo p : t.partitions()) {
                TopicPartition tp = new TopicPartition(t.name(), p.partition());
                if (p.leader() >= 0) {
                    leaders.put(tp, p.leader());
                } else {
                    leaders.remove(tp);
                }
            }
        }
        return m;
    }

    public MetadataResponse refreshMetadata() throws IOException {
        return refreshMetadata(List.of());
    }

    /** Cached leader of {@code tp}, or -1. */
    public int leader(TopicPartition tp) {
        return leaders.getOrDefault(tp, -1);
    }

    /** Remember a leader learnt from an error hint ({@code -1} forgets it). */
    public void setLeader(TopicPartition tp, int brokerId) {
        if (brokerId >= 0 && brokers.containsKey(brokerId)) {
            leaders.put(tp, brokerId);
        } else {
            leaders.remove(tp);
        }
    }

    /** Partition count from the cache (refreshing once if unknown), or -1 if the topic doesn't exist. */
    public int partitionCount(String topic) throws IOException {
        Integer n = partitionCounts.get(topic);
        if (n == null) {
            refreshMetadata(List.of(topic));
            n = partitionCounts.get(topic);
        }
        return n == null ? -1 : n;
    }

    public int controller() {
        return controller;
    }

    public int coordinator() {
        return coordinator;
    }

    public void setCoordinator(int id) {
        coordinator = id;
    }

    public void setController(int id) {
        controller = id;
    }

    public Map<Integer, InetSocketAddress> brokers() {
        return new HashMap<>(brokers);
    }

    InetSocketAddress address(int brokerId) {
        return brokers.get(brokerId);
    }

    @Override
    public void close() {
        closed = true;
        for (BrokerConnection c : conns.values()) {
            c.close();
        }
        conns.clear();
    }
}
