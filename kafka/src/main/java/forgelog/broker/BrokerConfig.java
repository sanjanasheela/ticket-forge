package forgelog.broker;

import forgelog.log.CommitLog;
import forgelog.raft.RaftNode;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

/**
 * @param nodeId this broker's id (a key of {@code cluster})
 * @param cluster every broker of the cluster, including this one: id → address
 * @param fsync fsync logs before acknowledging (otherwise durability comes from replication alone, like Kafka's default)
 */
public record BrokerConfig(int nodeId, Map<Integer, InetSocketAddress> cluster, Path dataDir, boolean fsync,
        CommitLog.Config log, RaftNode.Timing timing) {

    public BrokerConfig {
        cluster = new TreeMap<>(cluster);
        if (!cluster.containsKey(nodeId)) {
            throw new IllegalArgumentException("node " + nodeId + " is not in the cluster " + cluster.keySet());
        }
    }

    public InetSocketAddress self() {
        return cluster.get(nodeId);
    }

    public int[] members() {
        return cluster.keySet().stream().mapToInt(Integer::intValue).toArray();
    }

    /** {@code "1=127.0.0.1:9091,2=127.0.0.1:9092"} → id → address. */
    public static Map<Integer, InetSocketAddress> parseCluster(String spec) {
        Map<Integer, InetSocketAddress> out = new TreeMap<>();
        for (String part : spec.split(",")) {
            String p = part.trim();
            if (p.isEmpty()) {
                continue;
            }
            int eq = p.indexOf('=');
            int colon = p.lastIndexOf(':');
            if (eq <= 0 || colon <= eq) {
                throw new IllegalArgumentException("bad cluster entry '" + p + "' (expected id=host:port)");
            }
            int id = Integer.parseInt(p.substring(0, eq));
            String host = p.substring(eq + 1, colon);
            int port = Integer.parseInt(p.substring(colon + 1));
            out.put(id, new InetSocketAddress(host, port));
        }
        return out;
    }

    public static BrokerConfig of(int nodeId, Map<Integer, InetSocketAddress> cluster, Path dataDir) {
        return new BrokerConfig(nodeId, cluster, dataDir, true, CommitLog.Config.DEFAULT, RaftNode.Timing.DEFAULT);
    }
}
