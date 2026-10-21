package forgelog;

import forgelog.broker.Broker;
import forgelog.broker.BrokerConfig;
import forgelog.client.Admin;
import forgelog.log.CommitLog;
import forgelog.protocol.Messages.GroupStatus;
import forgelog.protocol.Messages.StatusResponse;
import forgelog.raft.RaftNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;

/** N in-process brokers on loopback, each with its own data directory. */
final class TestCluster implements AutoCloseable {
    final Map<Integer, InetSocketAddress> addrs = new TreeMap<>();
    final Map<Integer, Broker> brokers = new TreeMap<>();
    final Path dir;

    TestCluster(int n, Path dir) {
        this.dir = dir;
        for (int i = 1; i <= n; i++) {
            addrs.put(i, new InetSocketAddress("127.0.0.1", freePort()));
        }
        for (int id : addrs.keySet()) {
            start(id);
        }
    }

    private static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    BrokerConfig config(int id) {
        return new BrokerConfig(id, addrs, dir.resolve("broker-" + id), true, new CommitLog.Config(1 << 20, 1 << 16),
                RaftNode.Timing.DEFAULT);
    }

    void start(int id) {
        try {
            brokers.put(id, Broker.start(config(id)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void stop(int id) {
        Broker b = brokers.remove(id);
        if (b != null) {
            b.close();
        }
    }

    String bootstrap() {
        StringJoiner j = new StringJoiner(",");
        for (InetSocketAddress a : addrs.values()) {
            j.add(a.getHostString() + ":" + a.getPort());
        }
        return j.toString();
    }

    /** Leader of a group according to the running brokers (-1 if they don't agree on one). */
    int leaderOf(String name, Admin admin) throws IOException {
        int leader = -1;
        long term = -1;
        for (int id : brokers.keySet()) {
            StatusResponse s = admin.status(id);
            for (GroupStatus g : s.groups()) {
                if (g.name().equals(name) && g.role().equals("LEADER") && g.term() > term) {
                    leader = id;
                    term = g.term();
                }
            }
        }
        return leader;
    }

    static void eventually(String what, long timeoutMs, BooleanSupplier cond) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000;
        while (!cond.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for: " + what);
            }
            Thread.sleep(20);
        }
    }

    @Override
    public void close() {
        for (Broker b : brokers.values()) {
            b.close();
        }
        brokers.clear();
    }
}
