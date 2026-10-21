package forgelog.raft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RaftNodeTest {
    @Test
    void electsExactlyOneLeader() {
        SimCluster c = new SimCluster(3, 1);
        int l = c.awaitLeader(2_000);
        c.run(1_000);
        assertEquals(l, c.leader(), "leadership is stable without failures");
        for (int i = 0; i < 3; i++) {
            assertEquals(c.nodes[l].term(), c.nodes[i].term());
            assertEquals(l, c.nodes[i].leaderId());
        }
    }

    @Test
    void replicatesAndCommitsOnEveryNode() throws Exception {
        SimCluster c = new SimCluster(3, 2);
        int l = c.awaitLeader(2_000);
        List<String> sent = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            c.propose(l, "v" + i);
            sent.add("v" + i);
            if (i % 50 == 0) {
                c.run(1);
            }
        }
        c.run(500);
        for (int i = 0; i < 3; i++) {
            assertEquals(sent, c.committedValues(i), "node " + i);
        }
        c.assertConverged();
    }

    @Test
    void followersRejectProposals() {
        SimCluster c = new SimCluster(3, 3);
        int l = c.awaitLeader(2_000);
        int f = (l + 1) % 3;
        NotLeaderException e = assertThrows(NotLeaderException.class, () -> c.propose(f, "x"));
        assertEquals(l, e.leaderHint);
    }

    @Test
    void leaderCrashFailsOverFastWithoutLosingCommittedEntries() throws Exception {
        SimCluster c = new SimCluster(3, 4);
        int l = c.awaitLeader(2_000);
        for (int i = 0; i < 100; i++) {
            c.propose(l, "a" + i);
        }
        c.run(200);
        List<String> before = c.committedValues(l);
        assertEquals(100, before.size());

        long crashedAt = c.now;
        c.crash(l);
        int l2 = c.awaitLeader(2_000);
        long failover = c.now - crashedAt;
        assertNotEquals(l, l2);
        // Detection takes one election timeout (150-300 ms), then pre-vote + vote.
        assertTrue(failover < 700, "failover took " + failover + " ms");
        assertEquals(before, c.committedValues(l2), "committed entries survive the leader");

        for (int i = 0; i < 50; i++) {
            c.propose(l2, "b" + i);
        }
        c.run(200);
        c.restart(l);
        c.run(1_000);
        assertEquals(150, c.committedValues(l).size(), "restarted node catches up");
        c.assertConverged();
    }

    @Test
    void partitionedLeaderStepsDownAndItsUncommittedWritesAreDiscarded() throws Exception {
        SimCluster c = new SimCluster(3, 5);
        int l = c.awaitLeader(2_000);
        c.propose(l, "committed");
        c.run(200);

        c.isolate(l);
        c.propose(l, "lost-1"); // accepted by the isolated leader, can never commit
        c.propose(l, "lost-2");
        c.run(SimCluster.TIMING.electionMaxMs() + 100);
        assertFalse(c.nodes[l].isLeader(), "check-quorum: a leader cut off from the majority steps down");

        int l2 = c.awaitLeader(2_000);
        assertNotEquals(l, l2);
        c.propose(l2, "after-partition");
        c.run(200);

        c.heal();
        c.run(1_000);
        for (int i = 0; i < 3; i++) {
            assertEquals(List.of("committed", "after-partition"), c.committedValues(i), "node " + i);
        }
        c.assertConverged();
    }

    @Test
    void minorityCannotCommit() throws Exception {
        SimCluster c = new SimCluster(5, 6);
        int l = c.awaitLeader(2_000);
        // Leader plus one follower on one side, three nodes on the other.
        int buddy = (l + 1) % 5;
        for (int i = 0; i < 5; i++) {
            for (int k = 0; k < 5; k++) {
                boolean sideI = i == l || i == buddy;
                boolean sideK = k == l || k == buddy;
                c.cut[i][k] = sideI != sideK;
            }
        }
        long commitBefore = c.nodes[l].commitIndex();
        c.propose(l, "minority");
        c.run(100);
        assertEquals(commitBefore, c.nodes[l].commitIndex(), "2 of 5 is not a quorum");
        c.run(1_000);
        int l2 = c.leader();
        assertTrue(l2 >= 0 && l2 != l && l2 != buddy, "the majority side elects its own leader");
        c.heal();
        c.run(1_000);
        assertFalse(c.committedValues(l).contains("minority"));
        c.assertConverged();
    }

    @Test
    void preVoteStopsARejoiningNodeFromDisruptingTheLeader() {
        SimCluster c = new SimCluster(3, 7);
        int l = c.awaitLeader(2_000);
        long term = c.nodes[l].term();
        int f = (l + 1) % 3;
        c.isolate(f);
        c.run(3_000); // f times out over and over, but only ever pre-votes
        assertEquals(term, c.nodes[f].term(), "pre-vote never bumps the term");
        c.heal();
        c.run(1_000);
        assertEquals(l, c.leader(), "the leader was not deposed");
        assertEquals(term, c.nodes[l].term());
    }

    @Test
    void singleNodeGroupCommitsOnItsOwn() throws Exception {
        SimCluster c = new SimCluster(1, 8);
        int l = c.awaitLeader(1_000);
        c.propose(l, "solo");
        c.run(1);
        assertEquals(List.of("solo"), c.committedValues(l));
    }

    @Test
    void divergentFollowerLogsAreRepaired() throws Exception {
        SimCluster c = new SimCluster(5, 9);
        for (int round = 0; round < 6; round++) {
            int l = c.awaitLeader(3_000);
            c.isolate(l);
            for (int i = 0; i < 20; i++) {
                c.propose(l, "orphan-" + round + "-" + i); // never committed
            }
            c.run(600);
            c.heal();
        }
        int l = c.awaitLeader(3_000);
        c.propose(l, "final");
        c.run(1_000);
        c.assertConverged();
        for (int i = 0; i < 5; i++) {
            List<String> v = c.committedValues(i);
            assertEquals("final", v.getLast());
            assertTrue(v.stream().noneMatch(s -> s.startsWith("orphan")), "orphans never commit: " + v);
        }
    }

    /**
     * Random crashes, restarts, partitions and message loss for a long
     * simulated period. Invariants are checked every millisecond; at the end
     * every value whose proposal was committed must be in every log, once.
     */
    @Test
    void randomizedChaosKeepsSafety() {
        for (long seed = 100; seed < 106; seed++) {
            SimCluster c = new SimCluster(5, seed);
            Random r = new Random(seed);
            c.dropRate = 0.05;
            int next = 0;
            for (int step = 0; step < 300; step++) {
                int action = r.nextInt(10);
                int node = r.nextInt(5);
                switch (action) {
                    case 0 -> {
                        int down = 0;
                        for (boolean d : c.down) {
                            down += d ? 1 : 0;
                        }
                        if (down < 2) {
                            c.crash(node);
                        }
                    }
                    case 1 -> {
                        if (c.down[node]) {
                            c.restart(node);
                        }
                    }
                    case 2 -> c.isolate(node);
                    case 3 -> c.heal();
                    default -> {
                        int l = c.leader();
                        if (l >= 0) {
                            for (int k = 0; k < 5; k++) {
                                try {
                                    c.propose(l, "x" + next++);
                                } catch (NotLeaderException e) {
                                    break;
                                }
                            }
                        }
                    }
                }
                c.run(20 + r.nextInt(80));
            }
            c.heal();
            c.dropRate = 0;
            for (int i = 0; i < 5; i++) {
                if (c.down[i]) {
                    c.restart(i);
                }
            }
            int l = c.awaitLeader(5_000);
            try {
                c.propose(l, "end");
            } catch (NotLeaderException e) {
                throw new AssertionError(e);
            }
            c.run(2_000);
            c.assertConverged();
            List<String> values = c.committedValues(0);
            Set<String> unique = new HashSet<>(values);
            assertEquals(values.size(), unique.size(), "no entry appears twice (seed " + seed + ")");
            assertEquals("end", values.getLast());
            assertTrue(values.size() > 200, "chaos run committed only " + values.size() + " values (seed " + seed + ")");
        }
    }
}
