package forgelog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BrokerTests {

    @TempDir
    Path dir;

    private Broker start(int partitions) throws Exception {
        return new Broker(dir, 0, partitions, true, 3_000);
    }

    private static Client.Msg msg(String topic, String key, String value) {
        return new Client.Msg(topic, key, "T", "id-" + value, value);
    }

    private static void await(java.util.function.BooleanSupplier c, long ms) throws InterruptedException {
        long end = System.currentTimeMillis() + ms;
        while (!c.getAsBoolean() && System.currentTimeMillis() < end) {
            Thread.sleep(20);
        }
        assertTrue(c.getAsBoolean(), "condition not met in " + ms + " ms");
    }

    @Test
    void sameKeyStaysInOrderOnOnePartition() throws Exception {
        try (Broker b = start(3); Client c = new Client("localhost:" + b.port())) {
            List<Client.Msg> msgs = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                msgs.add(msg("t", "k1", Integer.toString(i)));
            }
            c.produce("t", msgs);
            List<Rec> all = new ArrayList<>();
            for (int p = 0; p < 3; p++) {
                all.addAll(c.fetch("t", p, 0, 100, 50));
            }
            assertEquals(50, all.size());                                   // all on one partition
            for (int i = 0; i < 50; i++) {
                assertEquals(Integer.toString(i), all.get(i).valueString());
                assertEquals(i, all.get(i).offset());
                assertEquals("id-" + i, all.get(i).eventId());
            }
        }
    }

    @Test
    void recordsSurviveARestartAndATornTailIsCutOff() throws Exception {
        try (Broker b = start(1); Client c = new Client("localhost:" + b.port())) {
            c.produce("t", List.of(msg("t", "k", "a"), msg("t", "k", "b"), msg("t", "k", "c")));
        }
        Path log = dir.resolve("t/0/log");
        long size = Files.size(log);
        Files.write(log, new byte[] {0, 0, 0, 90, 1, 2, 3}, StandardOpenOption.APPEND);   // a half-written frame
        try (Broker b = start(1); Client c = new Client("localhost:" + b.port())) {
            assertEquals(size, Files.size(log));                            // garbage truncated away
            List<Rec> recs = c.fetch("t", 0, 0, 10, 50);
            assertEquals(List.of("a", "b", "c"), recs.stream().map(Rec::valueString).toList());
            assertEquals(3, c.produce("t", List.of(msg("t", "k", "d")))[0]);   // appends continue at the right offset
        }
    }

    @Test
    void longPollWakesWhenARecordArrives() throws Exception {
        try (Broker b = start(1); Client c = new Client("localhost:" + b.port()); Client p = new Client("localhost:" + b.port())) {
            Thread.ofVirtual().start(() -> {
                try {
                    Thread.sleep(200);
                    p.produce("t", List.of(msg("t", null, "late")));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            long t0 = System.nanoTime();
            List<Rec> recs = c.fetch("t", 0, 0, 10, 5_000);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertEquals(1, recs.size());
            assertTrue(ms < 2_000, "returned after " + ms + " ms, should be about 200");
        }
    }

    @Test
    void committedOffsetsSurviveARestart() throws Exception {
        try (Broker b = start(1); Client c = new Client("localhost:" + b.port())) {
            assertEquals(-1, c.committed("g", "t", 0));
            c.commit("g", "t", 0, 7);
        }
        try (Broker b = start(1); Client c = new Client("localhost:" + b.port())) {
            assertEquals(7, c.committed("g", "t", 0));
        }
    }

    @Test
    void groupMembersSplitThePartitionsAndEveryRecordIsReadOnce() throws Exception {
        try (Broker b = start(4)) {
            String addr = "localhost:" + b.port();
            Set<String> seen = ConcurrentHashMap.newKeySet();
            AtomicInteger deliveries = new AtomicInteger();
            GroupConsumer.Handler h = r -> {
                seen.add(r.valueString());
                deliveries.incrementAndGet();
            };
            try (GroupConsumer c1 = new GroupConsumer(addr, "g", "t", h); GroupConsumer c2 = new GroupConsumer(addr, "g", "t", h);
                 Producer p = new Producer(addr)) {
                Thread.sleep(3_000);        // let both members join and the first rebalance settle
                for (int i = 0; i < 400; i++) {
                    p.send(msg("t", "key" + i, Integer.toString(i))).get();
                }
                await(() -> seen.size() == 400, 15_000);
                Thread.sleep(500);
                assertEquals(400, deliveries.get(), "no duplicates while membership is stable");
            }
        }
    }

    @Test
    void aFailingHandlerIsRetriedAndNothingIsSkipped() throws Exception {
        try (Broker b = start(1)) {
            String addr = "localhost:" + b.port();
            AtomicInteger attempts = new AtomicInteger();
            List<String> done = new CopyOnWriteArrayList<>();
            try (GroupConsumer c = new GroupConsumer(addr, "g", "t", r -> {
                if (r.valueString().equals("bad") && attempts.incrementAndGet() < 3) {
                    throw new IllegalStateException("boom");
                }
                done.add(r.valueString());
            }); Producer p = new Producer(addr)) {
                p.send(msg("t", "k", "ok1")).get();
                p.send(msg("t", "k", "bad")).get();
                p.send(msg("t", "k", "ok2")).get();
                await(() -> done.size() == 3, 15_000);
                assertEquals(List.of("ok1", "bad", "ok2"), done);           // order kept, nothing skipped
            }
        }
    }

    @Test
    void aSurvivorTakesOverAfterAMemberLeaves() throws Exception {
        try (Broker b = start(2)) {
            String addr = "localhost:" + b.port();
            Set<String> seen = Collections.newSetFromMap(new ConcurrentHashMap<>());
            GroupConsumer.Handler h = r -> seen.add(r.valueString());
            try (GroupConsumer survivor = new GroupConsumer(addr, "g", "t", h); Producer p = new Producer(addr)) {
                GroupConsumer leaver = new GroupConsumer(addr, "g", "t", h);
                Thread.sleep(500);
                leaver.close();
                for (int i = 0; i < 100; i++) {
                    p.send(msg("t", "k" + i, Integer.toString(i))).get();
                }
                await(() -> seen.size() == 100, 20_000);
            }
        }
    }
}
