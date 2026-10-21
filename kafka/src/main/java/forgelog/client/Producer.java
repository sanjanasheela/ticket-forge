package forgelog.client;

import forgelog.protocol.ApiKey;
import forgelog.protocol.Errors;
import forgelog.protocol.Messages.ProducePartition;
import forgelog.protocol.Messages.ProduceRecord;
import forgelog.protocol.Messages.ProduceRequest;
import forgelog.protocol.Messages.ProduceResponse;
import forgelog.protocol.Messages.ProduceResult;
import forgelog.protocol.TopicPartition;
import forgelog.util.Log;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

/**
 * An asynchronous, batching, <b>idempotent</b> producer.
 *
 * <p>{@link #send} queues a record and returns a future that completes once a
 * quorum of the partition's replicas has it (Raft commit), with the record's
 * offset. A background sender drains the queues, one request per leader
 * broker carrying every ready partition.
 *
 * <p>Idempotence: each partition queue has its own producer id and numbers its
 * records 0, 1, 2... A record stays at the head of its queue until it is
 * acknowledged, so retries after a timeout, a lost connection or a leader
 * change resend the <em>same</em> sequence numbers, and the leader answers
 * duplicates with the offset they already have. Records are never written
 * twice and never reordered.
 */
public final class Producer implements Closeable {
    /**
     * @param maxBatchRecords records per partition per request
     * @param maxRequestBytes approximate bound on one request
     * @param requestTimeoutMs how long the broker may take to commit a request
     * @param deliveryTimeoutMs give up on a record (failing its future) after this long
     */
    public record Config(int maxBatchRecords, int maxRequestBytes, int requestTimeoutMs, int deliveryTimeoutMs) {
        public static final Config DEFAULT = new Config(2_000, 1 << 20, 10_000, 60_000);
    }

    public record RecordMetadata(String topic, int partition, long offset) {}

    private static final class Pending {
        final ProduceRecord record;
        final CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        final long deadline;

        Pending(ProduceRecord record, long deadline) {
            this.record = record;
            this.deadline = deadline;
        }
    }

    private static final class PartitionQueue {
        final TopicPartition tp;
        final ArrayDeque<Pending> records = new ArrayDeque<>();
        long producerId;
        int nextSequence;
        long retryAt;

        PartitionQueue(TopicPartition tp, long producerId) {
            this.tp = tp;
            this.producerId = producerId;
        }
    }

    private static final SecureRandom IDS = new SecureRandom();

    private final ClusterClient cluster;
    private final Config cfg;
    private final Object lock = new Object();
    private final Map<TopicPartition, PartitionQueue> queues = new LinkedHashMap<>();
    private final Map<String, AtomicInteger> roundRobin = new HashMap<>();
    private int queued;
    private boolean inFlight;
    private volatile boolean closed;
    private final Thread sender;
    private final java.util.concurrent.ExecutorService io = java.util.concurrent.Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "forgelog-producer-io");
        t.setDaemon(true);
        return t;
    });

    public Producer(String bootstrapServers) {
        this(bootstrapServers, Config.DEFAULT);
    }

    public Producer(String bootstrapServers, Config cfg) {
        this.cluster = new ClusterClient(bootstrapServers);
        this.cfg = cfg;
        this.sender = new Thread(this::runSender, "forgelog-producer");
        sender.setDaemon(true);
        sender.start();
    }

    private static long newProducerId() {
        return IDS.nextLong() & Long.MAX_VALUE;
    }

    static int partitionFor(byte[] key, int partitions) {
        CRC32 crc = new CRC32();
        crc.update(key);
        return (int) ((crc.getValue() & 0x7fffffff) % partitions);
    }

    /** Send to a partition chosen by key hash (round robin for a null key). */
    public CompletableFuture<RecordMetadata> send(String topic, byte[] key, byte[] value) {
        int n;
        try {
            n = cluster.partitionCount(topic);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(new ForgeLogException("metadata unavailable", null, e));
        }
        if (n <= 0) {
            return CompletableFuture.failedFuture(
                    new ForgeLogException(Errors.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic " + topic));
        }
        int p;
        if (key != null) {
            p = partitionFor(key, n);
        } else {
            synchronized (roundRobin) {
                p = Math.floorMod(roundRobin.computeIfAbsent(topic, t -> new AtomicInteger()).getAndIncrement(), n);
            }
        }
        return send(topic, p, key, value);
    }

    public CompletableFuture<RecordMetadata> send(String topic, String key, String value) {
        return send(topic, key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    /** Send to a specific partition. */
    public CompletableFuture<RecordMetadata> send(String topic, int partition, byte[] key, byte[] value) {
        if (closed) {
            return CompletableFuture.failedFuture(new ForgeLogException("producer closed"));
        }
        TopicPartition tp = new TopicPartition(topic, partition);
        synchronized (lock) {
            PartitionQueue q = queues.computeIfAbsent(tp, t -> new PartitionQueue(t, newProducerId()));
            Pending p = new Pending(new ProduceRecord(q.producerId, q.nextSequence++, System.currentTimeMillis(),
                    key, value), System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(cfg.deliveryTimeoutMs()));
            q.records.add(p);
            queued++;
            lock.notifyAll();
            return p.future;
        }
    }

    /** Block until every record sent so far has completed (successfully or not). */
    public void flush() {
        synchronized (lock) {
            while (queued > 0 || inFlight) {
                try {
                    lock.wait(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // sender
    // -----------------------------------------------------------------------

    private record Batch(PartitionQueue queue, List<Pending> records) {}

    private void runSender() {
        long metadataAt = 0;
        while (true) {
            Map<Integer, List<Batch>> byLeader = new LinkedHashMap<>();
            boolean needMetadata = false;
            synchronized (lock) {
                while (queued == 0 && !closed) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                if (queued == 0) {
                    return; // closed and drained
                }
                long now = System.nanoTime();
                expire(now);
                for (PartitionQueue q : queues.values()) {
                    if (q.records.isEmpty() || now < q.retryAt) {
                        continue;
                    }
                    int leader = cluster.leader(q.tp);
                    if (leader < 0) {
                        needMetadata = true;
                        continue;
                    }
                    List<Pending> batch = new ArrayList<>();
                    int bytes = 0;
                    for (Pending p : q.records) {
                        if (batch.size() >= cfg.maxBatchRecords() || bytes > cfg.maxRequestBytes()) {
                            break;
                        }
                        batch.add(p);
                        bytes += 32 + len(p.record.key()) + len(p.record.value());
                    }
                    byLeader.computeIfAbsent(leader, k -> new ArrayList<>()).add(new Batch(q, batch));
                }
                inFlight = !byLeader.isEmpty();
            }
            if (needMetadata && System.nanoTime() - metadataAt > TimeUnit.MILLISECONDS.toNanos(50)) {
                metadataAt = System.nanoTime();
                try {
                    cluster.refreshMetadata();
                } catch (IOException e) {
                    Log.debug("producer: metadata refresh failed: " + e);
                }
            }
            if (byLeader.size() == 1) {
                Map.Entry<Integer, List<Batch>> e = byLeader.entrySet().iterator().next();
                sendTo(e.getKey(), e.getValue());
            } else if (!byLeader.isEmpty()) {
                // One request per leader, all in flight at once: their commit latencies overlap.
                List<java.util.concurrent.Future<?>> fs = new ArrayList<>();
                for (Map.Entry<Integer, List<Batch>> e : byLeader.entrySet()) {
                    fs.add(io.submit(() -> sendTo(e.getKey(), e.getValue())));
                }
                for (java.util.concurrent.Future<?> f : fs) {
                    try {
                        f.get();
                    } catch (java.util.concurrent.ExecutionException | InterruptedException ex) {
                        Log.debug("producer: send task failed: " + ex);
                    }
                }
            }
            synchronized (lock) {
                inFlight = false;
                lock.notifyAll();
            }
            if (byLeader.isEmpty()) {
                sleep(needMetadata ? 20 : 5);
            }
        }
    }

    private static int len(byte[] b) {
        return b == null ? 0 : b.length;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void sendTo(int leader, List<Batch> batches) {
        List<ProducePartition> parts = new ArrayList<>();
        for (Batch b : batches) {
            List<ProduceRecord> recs = new ArrayList<>(b.records().size());
            for (Pending p : b.records()) {
                recs.add(p.record);
            }
            parts.add(new ProducePartition(b.queue().tp.topic(), b.queue().tp.partition(), recs));
        }
        ProduceResponse resp;
        try {
            resp = ProduceResponse.decode(cluster.call(leader, ApiKey.PRODUCE,
                    new ProduceRequest(cfg.requestTimeoutMs(), parts)::encode, cfg.requestTimeoutMs() + 5_000));
        } catch (IOException | RuntimeException e) {
            // Leader unreachable (crashed?): forget it, back off, retry the same sequences elsewhere.
            Log.debug("producer: request to broker " + leader + " failed: " + e);
            synchronized (lock) {
                for (Batch b : batches) {
                    cluster.setLeader(b.queue().tp, -1);
                    b.queue().retryAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50);
                }
            }
            return;
        }
        synchronized (lock) {
            for (int i = 0; i < batches.size(); i++) {
                Batch b = batches.get(i);
                ProduceResult r = i < resp.results().size() ? resp.results().get(i) : null;
                PartitionQueue q = b.queue();
                if (r != null && r.error() == Errors.NONE) {
                    for (int k = 0; k < b.records().size(); k++) {
                        Pending p = q.records.poll();
                        queued--;
                        p.future.complete(new RecordMetadata(q.tp.topic(), q.tp.partition(), r.offsets()[k]));
                    }
                } else if (r != null && !r.error().retriable) {
                    failQueue(q, new ForgeLogException(r.error(), "produce to " + q.tp + " failed: " + r.error()));
                } else {
                    cluster.setLeader(q.tp, r != null && r.error() == Errors.NOT_LEADER ? r.leaderHint() : -1);
                    q.retryAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(r == null ? 50 : 20);
                }
            }
        }
    }

    /**
     * Fail every queued record of a partition and start a new sequence
     * stream: after a give-up the broker may or may not have the last records,
     * so continuing the old numbering could be refused as out of order.
     */
    private void failQueue(PartitionQueue q, ForgeLogException e) {
        while (!q.records.isEmpty()) {
            q.records.poll().future.completeExceptionally(e);
            queued--;
        }
        q.producerId = newProducerId();
        q.nextSequence = 0;
    }

    private void expire(long now) {
        for (PartitionQueue q : queues.values()) {
            Pending head = q.records.peek();
            if (head != null && now > head.deadline) {
                failQueue(q, new ForgeLogException(Errors.REQUEST_TIMED_OUT, "delivery timeout for " + q.tp));
            }
        }
    }

    /** Send what is queued, then stop. */
    @Override
    public void close() {
        flush();
        closed = true;
        synchronized (lock) {
            lock.notifyAll();
            for (Iterator<PartitionQueue> it = queues.values().iterator(); it.hasNext();) {
                failQueue(it.next(), new ForgeLogException("producer closed"));
            }
        }
        try {
            sender.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        io.shutdownNow();
        cluster.close();
    }
}
