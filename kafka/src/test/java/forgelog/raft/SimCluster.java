package forgelog.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import forgelog.log.MemoryLog;
import forgelog.log.Record;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * A deterministic, simulated Raft cluster: virtual time in 1 ms steps, a
 * network with random delay, loss and partitions, and crash/restart. Every
 * step checks Raft's safety properties:
 * <ul>
 *   <li>election safety: at most one leader per term;
 *   <li>state machine safety: once an index is committed anywhere, every node
 *       that commits that index commits the same entry.
 * </ul>
 */
final class SimCluster {
    static final RaftNode.Timing TIMING = new RaftNode.Timing(150, 300, 50, 64 * 1024, 1000);

    private record Msg(long at, long seq, int to, RaftMessage m) {}

    static final class MemHardState implements RaftNode.HardState {
        long term;
        int votedFor = -1;

        @Override
        public long term() {
            return term;
        }

        @Override
        public int votedFor() {
            return votedFor;
        }

        @Override
        public void save(long t, int v) {
            term = t;
            votedFor = v;
        }
    }

    final int n;
    final Random rng;
    final RaftNode[] nodes;
    final MemoryLog[] logs;
    final MemHardState[] hard;
    final boolean[] down;
    final boolean[][] cut;
    double dropRate;
    long now;
    private long seq;
    private final PriorityQueue<Msg> net =
            new PriorityQueue<>((a, b) -> a.at != b.at ? Long.compare(a.at, b.at) : Long.compare(a.seq, b.seq));
    private final Map<Long, Integer> leaderOfTerm = new HashMap<>();
    /** index -> the entry committed there (term, payload). */
    final Map<Long, String> committed = new HashMap<>();

    SimCluster(int n, long seed) {
        this.n = n;
        this.rng = new Random(seed);
        nodes = new RaftNode[n];
        logs = new MemoryLog[n];
        hard = new MemHardState[n];
        down = new boolean[n];
        cut = new boolean[n][n];
        for (int i = 0; i < n; i++) {
            logs[i] = new MemoryLog();
            hard[i] = new MemHardState();
            start(i);
        }
    }

    private void start(int i) {
        int[] members = new int[n];
        for (int k = 0; k < n; k++) {
            members[k] = k;
        }
        int id = i;
        nodes[i] = new RaftNode(7, i, members, logs[i], hard[i], (to, m) -> send(id, to, m),
                new RaftNode.Listener() {
                    @Override
                    public void onCommit(long commitIndex) {
                        recordCommit(id, commitIndex);
                    }
                }, TIMING, new Random(rng.nextLong()), now, -1);
    }

    private static String describe(Record r) {
        return r.term + ":" + (r.value == null ? "-" : new String(r.value, StandardCharsets.UTF_8));
    }

    private final Map<Integer, Long> checked = new HashMap<>();

    private void recordCommit(int node, long commitIndex) {
        for (long i = checked.getOrDefault(node, -1L) + 1; i <= commitIndex; i++) {
            String e = describe(logs[node].get(i));
            String prev = committed.putIfAbsent(i, e);
            if (prev != null && !prev.equals(e)) {
                throw new AssertionError("node " + node + " committed " + e + " at " + i + " but " + prev
                        + " was committed there before");
            }
        }
        checked.merge(node, commitIndex, Math::max);
    }

    private void send(int from, int to, RaftMessage m) {
        if (down[from] || down[to] || cut[from][to] || rng.nextDouble() < dropRate) {
            return;
        }
        net.add(new Msg(now + 1 + rng.nextInt(3), seq++, to, m));
    }

    /** Advance virtual time by {@code ms}, checking invariants each step. */
    void run(long ms) {
        for (long t = 0; t < ms; t++) {
            now++;
            while (!net.isEmpty() && net.peek().at <= now) {
                Msg msg = net.poll();
                if (!down[msg.to] && !cut[msg.m.from()][msg.to]) {
                    nodes[msg.to].receive(msg.m, now);
                }
            }
            for (int i = 0; i < n; i++) {
                if (down[i]) {
                    continue;
                }
                nodes[i].tick(now);
                nodes[i].flush(now);
                nodes[i].onPersisted(logs[i].lastIndex());
            }
            checkElectionSafety();
        }
    }

    private void checkElectionSafety() {
        for (int i = 0; i < n; i++) {
            if (!down[i] && nodes[i].isLeader()) {
                Integer prev = leaderOfTerm.putIfAbsent(nodes[i].term(), i);
                if (prev != null && prev != i) {
                    throw new AssertionError("two leaders in term " + nodes[i].term() + ": " + prev + " and " + i);
                }
            }
        }
    }

    /** The single live leader, or -1 (several leaders of different terms count as none). */
    int leader() {
        int found = -1;
        long bestTerm = -1;
        for (int i = 0; i < n; i++) {
            if (!down[i] && nodes[i].isLeader() && nodes[i].term() > bestTerm) {
                found = i;
                bestTerm = nodes[i].term();
            }
        }
        return found;
    }

    /** Run until a leader exists; returns it. */
    int awaitLeader(long maxMs) {
        for (long t = 0; t < maxMs; t++) {
            int l = leader();
            if (l >= 0 && nodes[l].isReady()) {
                return l;
            }
            run(1);
        }
        throw new AssertionError("no leader after " + maxMs + " ms");
    }

    long propose(int node, String value) throws NotLeaderException {
        return nodes[node].propose(Record.data(0, -1, -1, null, value.getBytes(StandardCharsets.UTF_8)));
    }

    void crash(int i) {
        down[i] = true;
    }

    /** Restart with the same durable log and hard state but fresh volatile state. */
    void restart(int i) {
        down[i] = false;
        start(i);
    }

    void isolate(int i) {
        for (int k = 0; k < n; k++) {
            if (k != i) {
                cut[i][k] = true;
                cut[k][i] = true;
            }
        }
    }

    void heal() {
        for (boolean[] row : cut) {
            java.util.Arrays.fill(row, false);
        }
    }

    /** Payloads of the committed data entries, in log order. */
    List<String> committedValues(int node) {
        List<String> out = new ArrayList<>();
        for (long i = 0; i <= nodes[node].commitIndex(); i++) {
            Record r = logs[node].get(i);
            if (r.isData()) {
                out.add(new String(r.value, StandardCharsets.UTF_8));
            }
        }
        return out;
    }

    /** After healing: every live node has the same committed log. */
    void assertConverged() {
        long commit = -1;
        for (int i = 0; i < n; i++) {
            if (!down[i]) {
                commit = Math.max(commit, nodes[i].commitIndex());
            }
        }
        for (int i = 0; i < n; i++) {
            if (down[i]) {
                continue;
            }
            assertEquals(commit, nodes[i].commitIndex(), "node " + i + " commit index");
            for (long k = 0; k <= commit; k++) {
                assertEquals(committed.get(k), describe(logs[i].get(k)), "node " + i + " entry " + k);
            }
        }
        assertTrue(commit >= 0);
    }
}
