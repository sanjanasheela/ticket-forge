package forgeraft;

import static org.junit.jupiter.api.Assertions.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;

class RaftTests {
    MemNet net; Map<String, Node> nodes; Map<String, List<String>> applied;
    final List<String> ids = List.of("n1", "n2", "n3", "n4", "n5");

    @BeforeEach void up() {
        net = new MemNet(); nodes = new LinkedHashMap<>(); applied = new ConcurrentHashMap<>();
        for (String id : ids) {
            List<String> peers = new ArrayList<>(ids); peers.remove(id);
            applied.put(id, new CopyOnWriteArrayList<>());
            Node n = new Node(id, peers, net, d -> applied.get(id).add(new String(d)), 150, 30);
            nodes.put(id, n); net.register(id, n);
        }
        nodes.values().forEach(Node::start);
    }
    @AfterEach void down() { nodes.values().forEach(Node::stop); }

    Node leader(Collection<String> among) throws Exception {
        long end = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < end) {
            List<Node> ls = new ArrayList<>();
            for (String id : among) if (nodes.get(id).role() == Node.Role.LEADER) ls.add(nodes.get(id));
            if (ls.size() == 1) return ls.get(0);
            Thread.sleep(20);
        }
        fail("no single leader among " + among); return null;
    }

    @Test void electsOneLeaderAndReplicates() throws Exception {
        Node l = leader(ids);
        for (int i = 0; i < 20; i++) l.propose(("v" + i).getBytes(), 2000);
        Thread.sleep(300);
        for (String id : ids) assertEquals(20, applied.get(id).size(), id);
        for (String id : ids) assertEquals(applied.get("n1"), applied.get(id));
    }

    @Test void leaderCrashTriggersReelectionAndNoLoss() throws Exception {
        Node l = leader(ids);
        for (int i = 0; i < 10; i++) l.propose(("a" + i).getBytes(), 2000);
        net.crash(l.id);
        List<String> rest = new ArrayList<>(ids); rest.remove(l.id);
        Node l2 = leader(rest);
        assertNotEquals(l.id, l2.id);
        for (int i = 0; i < 10; i++) l2.propose(("b" + i).getBytes(), 2000);
        net.recover(l.id);
        Thread.sleep(600);
        for (String id : ids) assertEquals(20, applied.get(id).size(), id);
        assertEquals(applied.get(l2.id), applied.get(l.id));
    }

    @Test void partitionedMinorityLeaderCannotCommitAndNoDivergence() throws Exception {
        Node old = leader(ids);
        old.propose("before".getBytes(), 2000);
        // isolate the leader plus one follower (minority of 2) from the other three
        String buddy = ids.stream().filter(i -> !i.equals(old.id)).findFirst().get();
        List<String> minority = List.of(old.id, buddy);
        List<String> majority = new ArrayList<>(ids); majority.removeAll(minority);
        net.partition(minority, majority);
        // old leader cannot reach quorum: the write must not commit
        assertThrows(Exception.class, () -> old.propose("lost".getBytes(), 600));
        Node fresh = leader(majority);
        for (int i = 0; i < 5; i++) fresh.propose(("m" + i).getBytes(), 2000);
        net.heal();
        Thread.sleep(800);
        // everyone converges on the majority's history; the minority write never appears
        for (String id : ids) { assertFalse(applied.get(id).contains("lost"), id + " applied a write that never reached quorum"); assertEquals(applied.get(fresh.id), applied.get(id), id); }
        assertEquals(6, applied.get("n1").size());
        // at most one leader per term at the end
        assertEquals(1, nodes.values().stream().filter(n -> n.role() == Node.Role.LEADER).count());
    }

    @Test void noQuorumMeansNoLeaderWrites() throws Exception {
        Node l = leader(ids);
        for (String id : ids) if (!id.equals(l.id)) net.crash(id);
        Thread.sleep(500);
        assertThrows(Exception.class, () -> l.propose("x".getBytes(), 500));
    }
}
