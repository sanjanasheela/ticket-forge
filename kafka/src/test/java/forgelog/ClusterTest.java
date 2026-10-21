package forgelog;

import static forgelog.TestCluster.eventually;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import forgelog.client.Admin;
import forgelog.client.ClusterClient;
import forgelog.client.Consumer;
import forgelog.client.Consumer.ConsumerRecord;
import forgelog.client.ForgeLogException;
import forgelog.client.Producer;
import forgelog.client.Producer.RecordMetadata;
import forgelog.protocol.ApiKey;
import forgelog.protocol.Errors;
import forgelog.protocol.Messages.GroupStatus;
import forgelog.protocol.Messages.ProducePartition;
import forgelog.protocol.Messages.ProduceRecord;
import forgelog.protocol.Messages.ProduceRequest;
import forgelog.protocol.Messages.ProduceResponse;
import forgelog.protocol.Messages.StatusResponse;
import forgelog.protocol.TopicPartition;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ClusterTest {
    @TempDir
    Path dir;

    private static Consumer.Config fastSession(int ms) {
        return new Consumer.Config(ms, 1_000, 1 << 20);
    }

    /** Consume from {@code group} until {@code n} records arrived (committing as it goes). */
    private static List<ConsumerRecord> consume(String bootstrap, String group, String topic, int n) {
        List<ConsumerRecord> out = new ArrayList<>();
        try (Consumer c = new Consumer(bootstrap, group, List.of(topic))) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (out.size() < n && System.nanoTime() < deadline) {
                out.addAll(c.poll(Duration.ofMillis(500)));
                c.commitSync();
            }
        }
        return out;
    }

    private static List<String> values(List<ConsumerRecord> recs) {
        return recs.stream().map(ConsumerRecord::valueString).toList();
    }

    @Test
    void producedRecordsAreConsumedInOrderPerPartition() throws Exception {
        try (TestCluster c = new TestCluster(3, dir); Admin admin = new Admin(c.bootstrap())) {
            admin.createTopic("orders", 3);
            Map<String, Integer> keyPartition = new HashMap<>();
            try (Producer p = new Producer(c.bootstrap())) {
                List<CompletableFuture<RecordMetadata>> fs = new ArrayList<>();
                for (int i = 0; i < 3_000; i++) {
                    fs.add(p.send("orders", "user-" + (i % 50), "order-" + i));
                }
                for (int i = 0; i < fs.size(); i++) {
                    RecordMetadata m = fs.get(i).get(30, TimeUnit.SECONDS);
                    Integer prev = keyPartition.put("user-" + (i % 50), m.partition());
                    assertTrue(prev == null || prev == m.partition(), "a key always maps to one partition");
                }
            }
            List<ConsumerRecord> got = consume(c.bootstrap(), "g1", "orders", 3_000);
            assertEquals(3_000, got.size());
            assertEquals(3_000, new HashSet<>(values(got)).size(), "no duplicates");
            Map<Integer, Long> lastOffset = new HashMap<>();
            Map<String, Integer> lastSeqOfKey = new HashMap<>();
            for (ConsumerRecord r : got) {
                Long prev = lastOffset.put(r.partition(), r.offset());
                assertTrue(prev == null || r.offset() > prev, "offsets increase within a partition");
                int seq = Integer.parseInt(r.valueString().substring(6));
                Integer prevSeq = lastSeqOfKey.put(r.keyString(), seq);
                assertTrue(prevSeq == null || seq > prevSeq, "per-key order is preserved");
            }
            assertEquals(3, lastOffset.size(), "keys spread over every partition");
        }
    }

    /**
     * The Black-Friday / leader-kill experiment: a producer writes
     * continuously while the partition leader is killed. Every acknowledged
     * record must survive, and no record may be written twice even though
     * the producer retries everything that was in flight.
     */
    @Test
    void killingTheLeaderLosesNoAcknowledgedRecordsAndWritesNoDuplicates() throws Exception {
        try (TestCluster c = new TestCluster(3, dir); Admin admin = new Admin(c.bootstrap())) {
            admin.createTopic("tickets", 1);
            int leader = c.leaderOf("tickets-0", admin);
            assertTrue(leader > 0);
            int total = 5_000;
            AtomicLong lastAckNanos = new AtomicLong();
            List<CompletableFuture<RecordMetadata>> fs = Collections.synchronizedList(new ArrayList<>());
            try (Producer p = new Producer(c.bootstrap())) {
                for (int i = 0; i < total; i++) {
                    CompletableFuture<RecordMetadata> f = p.send("tickets", 0, null,
                            ("seat-" + i).getBytes(StandardCharsets.UTF_8));
                    f.thenRun(() -> lastAckNanos.set(System.nanoTime()));
                    fs.add(f);
                    if (i == total / 3) {
                        fs.get(i).get(10, TimeUnit.SECONDS); // some acked before the kill
                        c.stop(leader);
                    }
                    if (i % 100 == 0) {
                        Thread.sleep(1);
                    }
                }
                Set<Long> offsets = new HashSet<>();
                for (CompletableFuture<RecordMetadata> f : fs) {
                    offsets.add(f.get(60, TimeUnit.SECONDS).offset()); // every send succeeds eventually
                }
                assertEquals(total, offsets.size(), "distinct offsets for distinct records");
            }
            int newLeader = c.leaderOf("tickets-0", admin);
            assertNotEquals(leader, newLeader);
            List<String> got = values(consume(c.bootstrap(), "audit", "tickets", total));
            assertEquals(total, got.size(), "exactly the acknowledged records, no duplicates");
            for (int i = 0; i < total; i++) {
                assertEquals("seat-" + i, got.get(i), "single partition keeps send order");
            }

            // The killed broker comes back as a follower and catches up.
            c.start(leader);
            eventually("restarted broker catches up", 10_000, () -> {
                try {
                    StatusResponse s = admin.status(leader);
                    GroupStatus g = s.groups().stream().filter(x -> x.name().equals("tickets-0")).findFirst()
                            .orElseThrow();
                    StatusResponse ls = admin.status(newLeader);
                    GroupStatus lg = ls.groups().stream().filter(x -> x.name().equals("tickets-0")).findFirst()
                            .orElseThrow();
                    return g.role().equals("FOLLOWER") && g.commitIndex() == lg.commitIndex();
                } catch (Exception e) {
                    return false;
                }
            });
        }
    }

    @Test
    void consumerGroupSplitsPartitionsAndResumesFromCommittedOffsets() throws Exception {
        try (TestCluster c = new TestCluster(3, dir); Admin admin = new Admin(c.bootstrap())) {
            admin.createTopic("events", 4);
            try (Producer p = new Producer(c.bootstrap())) {
                for (int i = 0; i < 400; i++) {
                    p.send("events", i % 4, null, ("e" + i).getBytes(StandardCharsets.UTF_8));
                }
                p.flush();
            }
            Consumer a = new Consumer(c.bootstrap(), "workers", List.of("events"), fastSession(3_000));
            Consumer b = new Consumer(c.bootstrap(), "workers", List.of("events"), fastSession(3_000));
            Set<String> seen = Collections.synchronizedSet(new HashSet<>());
            // Both members poll until the group settles with two partitions each.
            Thread tb = new Thread(() -> {
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (System.nanoTime() < end && b.assignment().size() != 2) {
                    b.poll(Duration.ofMillis(100)).forEach(r -> seen.add(r.valueString()));
                }
            });
            tb.start();
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (System.nanoTime() < end && (a.assignment().size() != 2 || b.assignment().size() != 2)) {
                a.poll(Duration.ofMillis(100)).forEach(r -> seen.add(r.valueString()));
            }
            tb.join();
            assertEquals(2, a.assignment().size());
            assertEquals(2, b.assignment().size());
            Set<TopicPartition> all = new HashSet<>(a.assignment());
            all.addAll(b.assignment());
            assertEquals(4, all.size(), "the group covers every partition exactly once");

            // A consumes and commits its partitions completely, then leaves.
            int settled = a.generation();
            long endA = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            Map<TopicPartition, Long> aPositions = a.positions();
            while (!(aPositions.size() == 2 && aPositions.values().stream().allMatch(o -> o >= 101))
                    && System.nanoTime() < endA) {
                a.poll(Duration.ofMillis(200)).forEach(r -> seen.add(r.valueString()));
                aPositions = a.positions();
            }
            assertEquals(settled, a.generation(), "no rebalance while the group is stable");
            assertEquals(2, aPositions.size());
            a.commitSync();
            List<TopicPartition> aParts = a.assignment();
            a.close();

            // B takes over A's partitions and starts where A's commit left them.
            Map<TopicPartition, Long> committed = b.committed(aParts);
            for (TopicPartition tp : aParts) {
                assertEquals(aPositions.get(tp), committed.get(tp), "A's commit is durable");
            }
            long endB = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while ((seen.size() < 400 || b.assignment().size() != 4) && System.nanoTime() < endB) {
                b.poll(Duration.ofMillis(200)).forEach(r -> seen.add(r.valueString()));
            }
            assertEquals(4, b.assignment().size(), "the remaining member owns everything");
            for (TopicPartition tp : aParts) {
                assertTrue(b.positions().get(tp) >= aPositions.get(tp), "B resumed at A's committed offset");
            }
            assertEquals(400, seen.size());
            b.close();
        }
    }

    @Test
    void aCrashedConsumersPartitionsMoveAfterItsSessionTimesOut() throws Exception {
        try (TestCluster c = new TestCluster(3, dir); Admin admin = new Admin(c.bootstrap())) {
            admin.createTopic("jobs", 2);
            Consumer a = new Consumer(c.bootstrap(), "pool", List.of("jobs"), fastSession(1_500));
            Consumer b = new Consumer(c.bootstrap(), "pool", List.of("jobs"), fastSession(1_500));
            Thread tb = new Thread(() -> {
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                while (System.nanoTime() < end && b.assignment().size() != 1) {
                    b.poll(Duration.ofMillis(100));
                }
            });
            tb.start();
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (System.nanoTime() < end && (a.assignment().size() != 1 || b.assignment().size() != 1)) {
                a.poll(Duration.ofMillis(100));
            }
            tb.join();
            assertEquals(1, a.assignment().size());
            a.abandon(); // no LeaveGroup, no more heartbeats
            long killed = System.nanoTime();
            while (b.assignment().size() != 2 && System.nanoTime() - killed < TimeUnit.SECONDS.toNanos(15)) {
                b.poll(Duration.ofMillis(100));
            }
            assertEquals(2, b.assignment().size(), "the dead member's partition was reassigned");
            b.close();
        }
    }

    /**
     * The split-brain experiment: the partition leader is cut off from both
     * followers. It must stop being leader (it can't reach a quorum), the
     * majority elects a new one and keeps accepting writes, and after the
     * partition heals the old leader rejoins with an identical log.
     */
    @Test
    void aPartitionedLeaderStepsDownAndTheMajorityCarriesOn() throws Exception {
        try (TestCluster c = new TestCluster(3, dir); Admin admin = new Admin(c.bootstrap())) {
            admin.createTopic("seats", 1);
            int leader = c.leaderOf("seats-0", admin);
            try (Producer p = new Producer(c.bootstrap())) {
                for (int i = 0; i < 100; i++) {
                    p.send("seats", 0, null, ("before-" + i).getBytes());
                }
                p.flush();

                List<Integer> others = new ArrayList<>(c.brokers.keySet());
                others.remove(Integer.valueOf(leader));
                admin.chaos(leader, others.stream().mapToInt(Integer::intValue).toArray());
                for (int o : others) {
                    admin.chaos(o, leader);
                }
                eventually("isolated leader steps down", 2_000, () -> {
                    try {
                        return admin.status(leader).groups().stream()
                                .filter(g -> g.name().equals("seats-0")).noneMatch(g -> g.role().equals("LEADER"));
                    } catch (Exception e) {
                        return false;
                    }
                });
                List<CompletableFuture<RecordMetadata>> during = new ArrayList<>();
                for (int i = 0; i < 100; i++) {
                    during.add(p.send("seats", 0, null, ("during-" + i).getBytes()));
                }
                for (CompletableFuture<RecordMetadata> f : during) {
                    f.get(20, TimeUnit.SECONDS); // the majority side accepts writes
                }
                int newLeader = c.leaderOf("seats-0", admin);
                assertTrue(others.contains(newLeader), "the new leader is on the majority side");

                for (int id : c.brokers.keySet()) {
                    admin.chaos(id);
                }
            }
            eventually("logs converge after healing", 10_000, () -> {
                try {
                    Set<Long> commits = new HashSet<>();
                    Set<Long> lasts = new HashSet<>();
                    for (int id : c.brokers.keySet()) {
                        GroupStatus g = admin.status(id).groups().stream().filter(x -> x.name().equals("seats-0"))
                                .findFirst().orElseThrow();
                        commits.add(g.commitIndex());
                        lasts.add(g.lastIndex());
                    }
                    return commits.size() == 1 && lasts.size() == 1;
                } catch (Exception e) {
                    return false;
                }
            });
            List<String> got = values(consume(c.bootstrap(), "check", "seats", 200));
            assertEquals(200, got.size());
            assertEquals("before-0", got.getFirst());
            assertEquals("during-99", got.getLast());
        }
    }

    @Test
    void aFullClusterRestartKeepsDataAndCommittedOffsets() throws Exception {
        try (TestCluster c = new TestCluster(3, dir)) {
            try (Admin admin = new Admin(c.bootstrap())) {
                admin.createTopic("ledger", 2);
            }
            try (Producer p = new Producer(c.bootstrap())) {
                for (int i = 0; i < 500; i++) {
                    p.send("ledger", i % 2, null, ("tx-" + i).getBytes());
                }
                p.flush();
            }
            List<ConsumerRecord> first = consume(c.bootstrap(), "billing", "ledger", 300);
            assertTrue(first.size() >= 300);

            for (int id : List.copyOf(c.brokers.keySet())) {
                c.stop(id);
            }
            for (int id : c.addrs.keySet()) {
                c.start(id);
            }

            try (Admin admin = new Admin(c.bootstrap())) {
                eventually("topic is back with leaders", 10_000, () -> {
                    try {
                        int[] leaders = admin.describeTopics().get("ledger");
                        return leaders != null && leaders[0] > 0 && leaders[1] > 0;
                    } catch (Exception e) {
                        return false;
                    }
                });
            }
            // Everything is still there...
            List<ConsumerRecord> all = consume(c.bootstrap(), "fresh-group", "ledger", 500);
            assertEquals(500, all.size());
            // ...and "billing" resumes after what it committed instead of starting over.
            List<ConsumerRecord> rest = consume(c.bootstrap(), "billing", "ledger", 500 - first.size());
            Set<String> union = new HashSet<>(values(first));
            union.addAll(values(rest));
            assertEquals(500, union.size());
            assertEquals(500, first.size() + rest.size(), "no record was delivered to billing twice");
        }
    }

    @Test
    void retriedProduceRequestsAreDeduplicated() throws Exception {
        try (TestCluster c = new TestCluster(3, dir); Admin admin = new Admin(c.bootstrap())) {
            admin.createTopic("pay", 1);
            ClusterClient cl = admin.cluster();
            cl.refreshMetadata();
            int leader = cl.leader(new TopicPartition("pay", 0));
            List<ProduceRecord> recs = new ArrayList<>();
            for (int s = 0; s < 10; s++) {
                recs.add(new ProduceRecord(4242, s, 0, null, ("charge-" + s).getBytes()));
            }
            ProduceRequest req = new ProduceRequest(5_000, List.of(new ProducePartition("pay", 0, recs)));
            ProduceResponse first = ProduceResponse.decode(cl.call(leader, ApiKey.PRODUCE, req::encode, 10_000));
            ProduceResponse again = ProduceResponse.decode(cl.call(leader, ApiKey.PRODUCE, req::encode, 10_000));
            assertEquals(Errors.NONE, first.results().getFirst().error());
            assertEquals(Errors.NONE, again.results().getFirst().error());
            for (int i = 0; i < 10; i++) {
                assertEquals(first.results().getFirst().offsets()[i], again.results().getFirst().offsets()[i],
                        "a duplicate is answered with the original offset");
            }
            // A gap in the sequence is refused.
            ProduceRequest gap = new ProduceRequest(5_000, List.of(new ProducePartition("pay", 0,
                    List.of(new ProduceRecord(4242, 15, 0, null, "x".getBytes())))));
            ProduceResponse r = ProduceResponse.decode(cl.call(leader, ApiKey.PRODUCE, gap::encode, 10_000));
            assertEquals(Errors.OUT_OF_ORDER_SEQUENCE, r.results().getFirst().error());

            assertEquals(10, consume(c.bootstrap(), "dedupe", "pay", 10).size());
            try (Consumer check = new Consumer(c.bootstrap(), "dedupe-2", List.of("pay"))) {
                List<ConsumerRecord> got = new ArrayList<>();
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                while (System.nanoTime() < end) {
                    got.addAll(check.poll(Duration.ofMillis(200)));
                }
                assertEquals(10, got.size(), "the log holds each record once");
            }
        }
    }

    @Test
    void aLongPollIsAnsweredAsSoonAsDataArrives() throws Exception {
        try (TestCluster c = new TestCluster(3, dir); Admin admin = new Admin(c.bootstrap())) {
            admin.createTopic("alerts", 1);
            try (Consumer cons = new Consumer(c.bootstrap(), "watchers", List.of("alerts"));
                    Producer p = new Producer(c.bootstrap())) {
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (cons.assignment().isEmpty() && System.nanoTime() < end) {
                    cons.poll(Duration.ofMillis(100));
                }
                Thread t = new Thread(() -> {
                    try {
                        Thread.sleep(300);
                        p.send("alerts", 0, null, "fire".getBytes()).get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                long start = System.nanoTime();
                t.start();
                List<ConsumerRecord> got = cons.poll(Duration.ofSeconds(5));
                long tookMs = (System.nanoTime() - start) / 1_000_000;
                t.join();
                assertEquals(List.of("fire"), values(got));
                assertTrue(tookMs < 2_000, "woken by the commit, not by the timeout: " + tookMs + " ms");
            }
        }
    }

    @Test
    void topicValidation() throws Exception {
        try (TestCluster c = new TestCluster(3, dir); Admin admin = new Admin(c.bootstrap())) {
            admin.createTopic("dup", 1);
            ForgeLogException e = assertThrows(ForgeLogException.class, () -> admin.createTopic("dup", 1));
            assertEquals(Errors.TOPIC_ALREADY_EXISTS, e.error);
            e = assertThrows(ForgeLogException.class, () -> admin.createTopic("__internal", 1));
            assertEquals(Errors.INVALID_TOPIC, e.error);
            e = assertThrows(ForgeLogException.class, () -> admin.createTopic("bad name!", 1));
            assertEquals(Errors.INVALID_TOPIC, e.error);
            try (Producer p = new Producer(c.bootstrap())) {
                CompletableFuture<RecordMetadata> f = p.send("missing", "k", "v");
                assertTrue(f.isCompletedExceptionally(), "unknown topics fail fast");
            }
            assertEquals(1, admin.describeTopics().get("dup").length);
        }
    }

    @Test
    void aSingleBrokerClusterWorks() throws Exception {
        try (TestCluster c = new TestCluster(1, dir); Admin admin = new Admin(c.bootstrap())) {
            admin.createTopic("solo", 2);
            try (Producer p = new Producer(c.bootstrap())) {
                for (int i = 0; i < 100; i++) {
                    p.send("solo", "k" + i, "v" + i);
                }
                p.flush();
            }
            assertEquals(100, consume(c.bootstrap(), "g", "solo", 100).size());
        }
    }
}
