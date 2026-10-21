package forgelog;

import forgelog.broker.Broker;
import forgelog.broker.BrokerConfig;
import forgelog.client.Admin;
import forgelog.client.Consumer;
import forgelog.client.Consumer.ConsumerRecord;
import forgelog.client.ForgeLogException;
import forgelog.client.Producer;
import forgelog.client.Producer.RecordMetadata;
import forgelog.log.CommitLog;
import forgelog.protocol.Errors;
import forgelog.protocol.Messages.GroupStatus;
import forgelog.protocol.Messages.StatusResponse;
import forgelog.raft.RaftNode;
import forgelog.util.Log;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** The {@code forgelog} command line: run brokers, manage topics, produce, consume, benchmark. */
public final class Main {
    static final String VERSION = "0.1.0";
    private static final String DEFAULT_BOOTSTRAP = "127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093";

    private static final String USAGE = """
            ForgeLog %s - a Raft-replicated commit-log broker (a custom Kafka)

            Usage: forgelog <command> [options]

            Brokers:
              broker   --id N --cluster 1=host:port,2=host:port,... --data-dir DIR
                       [--no-fsync] [--segment-mb 256] [--election-ms 150-300] [--heartbeat-ms 50]
              cluster  [--nodes 3] [--base-port 9091] [--data-dir data]      (N brokers in one process, for dev)

            Clients (all take --bootstrap host:port,...; default %s):
              topic create NAME [--partitions 3]
              topic list
              produce TOPIC [--key KEY]              one record per stdin line, prints partition@offset
              consume TOPIC [--group G] [--max N]    prints partition@offset key value, commits as it goes
              status                                 every broker's view of every Raft group
              chaos BROKER --block 2,3 | --heal      make BROKER drop Raft traffic with the given brokers
              bench [--topic T] [--partitions 3] [--records 200000] [--size 100] [--inflight N]
            """;

    private Main() {}

    public static void main(String[] argv) throws Exception {
        if (argv.length == 0 || argv[0].equals("-h") || argv[0].equals("--help")) {
            System.out.printf(USAGE, VERSION, DEFAULT_BOOTSTRAP);
            return;
        }
        if (argv[0].equals("-V") || argv[0].equals("--version")) {
            System.out.println("forgelog " + VERSION);
            return;
        }
        Args a = new Args(Arrays.copyOfRange(argv, 1, argv.length));
        try {
            switch (argv[0]) {
                case "broker" -> broker(a);
                case "cluster" -> cluster(a);
                case "topic" -> topic(a);
                case "produce" -> produce(a);
                case "consume" -> consume(a);
                case "status" -> status(a);
                case "chaos" -> chaos(a);
                case "bench" -> bench(a);
                default -> throw new IllegalArgumentException("unknown command '" + argv[0] + "'");
            }
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage() + "\n\nFor more information, try '--help'.");
            System.exit(2);
        } catch (ForgeLogException e) {
            System.err.println("error: " + e.getMessage());
            System.exit(1);
        }
    }

    /** {@code --flag value}, {@code --flag=value}, bare {@code --flag} and positional arguments. */
    private static final class Args {
        final Map<String, String> flags = new HashMap<>();
        final List<String> positional = new ArrayList<>();

        Args(String[] argv) {
            for (int i = 0; i < argv.length; i++) {
                String s = argv[i];
                if (s.startsWith("--")) {
                    int eq = s.indexOf('=');
                    if (eq > 0) {
                        flags.put(s.substring(2, eq), s.substring(eq + 1));
                    } else if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) {
                        flags.put(s.substring(2), argv[++i]);
                    } else {
                        flags.put(s.substring(2), "true");
                    }
                } else {
                    positional.add(s);
                }
            }
        }

        String get(String name, String def) {
            return flags.getOrDefault(name, def);
        }

        String required(String name) {
            String v = flags.get(name);
            if (v == null) {
                throw new IllegalArgumentException("--" + name + " is required");
            }
            return v;
        }

        int integer(String name, int def) {
            String v = flags.get(name);
            try {
                return v == null ? def : Integer.parseInt(v);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("--" + name + " expects a number, got '" + v + "'");
            }
        }

        boolean bool(String name) {
            return flags.containsKey(name);
        }

        String arg(int i, String what) {
            if (i >= positional.size()) {
                throw new IllegalArgumentException("missing " + what);
            }
            return positional.get(i);
        }

        String bootstrap() {
            return get("bootstrap", DEFAULT_BOOTSTRAP);
        }
    }

    private static BrokerConfig brokerConfig(int id, Map<Integer, InetSocketAddress> cluster, Path dir, Args a) {
        String election = a.get("election-ms", "150-300");
        String[] range = election.split("-");
        RaftNode.Timing t = new RaftNode.Timing(Integer.parseInt(range[0]),
                Integer.parseInt(range.length > 1 ? range[1] : range[0]), a.integer("heartbeat-ms", 50), 1 << 20,
                100_000);
        CommitLog.Config log = new CommitLog.Config((long) a.integer("segment-mb", 256) << 20, 1 << 20);
        return new BrokerConfig(id, cluster, dir, !a.bool("no-fsync"), log, t);
    }

    private static void awaitShutdown(List<Broker> brokers) throws InterruptedException {
        CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Log.info("shutting down");
            for (Broker b : brokers) {
                b.close();
            }
            done.countDown();
        }, "forgelog-shutdown"));
        done.await();
    }

    private static void broker(Args a) throws Exception {
        int id = Integer.parseInt(a.required("id"));
        Map<Integer, InetSocketAddress> cluster = BrokerConfig.parseCluster(a.required("cluster"));
        Broker b = Broker.start(brokerConfig(id, cluster, Path.of(a.required("data-dir")), a));
        awaitShutdown(List.of(b));
    }

    private static void cluster(Args a) throws Exception {
        int n = a.integer("nodes", 3);
        int base = a.integer("base-port", 9091);
        Path dir = Path.of(a.get("data-dir", "data"));
        Map<Integer, InetSocketAddress> cluster = new TreeMap<>();
        for (int i = 1; i <= n; i++) {
            cluster.put(i, new InetSocketAddress("127.0.0.1", base + i - 1));
        }
        List<Broker> brokers = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            brokers.add(Broker.start(brokerConfig(i, cluster, dir.resolve("broker-" + i), a)));
        }
        System.err.println("cluster of " + n + " brokers up; bootstrap: "
                + String.join(",", cluster.values().stream().map(x -> x.getHostString() + ":" + x.getPort()).toList()));
        awaitShutdown(brokers);
    }

    private static void topic(Args a) throws Exception {
        String sub = a.arg(0, "topic subcommand (create|list)");
        try (Admin admin = new Admin(a.bootstrap())) {
            switch (sub) {
                case "create" -> {
                    String name = a.arg(1, "topic name");
                    int p = a.integer("partitions", 3);
                    admin.createTopic(name, p);
                    System.out.println("created " + name + " with " + p + " partition(s)");
                }
                case "list" -> {
                    for (Map.Entry<String, int[]> e : admin.describeTopics().entrySet()) {
                        System.out.println(e.getKey() + "  partitions=" + e.getValue().length + "  leaders="
                                + Arrays.toString(e.getValue()));
                    }
                }
                default -> throw new IllegalArgumentException("unknown topic subcommand '" + sub + "'");
            }
        }
    }

    private static void produce(Args a) throws Exception {
        String topic = a.arg(0, "topic");
        String key = a.get("key", null);
        try (Producer p = new Producer(a.bootstrap());
                BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                RecordMetadata m = p.send(topic, key, line).get(60, TimeUnit.SECONDS);
                System.out.println(m.partition() + "@" + m.offset());
            }
        }
    }

    private static void consume(Args a) {
        String topic = a.arg(0, "topic");
        int max = a.integer("max", Integer.MAX_VALUE);
        int n = 0;
        try (Consumer c = new Consumer(a.bootstrap(), a.get("group", "cli"), List.of(topic))) {
            while (n < max) {
                for (ConsumerRecord r : c.poll(Duration.ofMillis(500))) {
                    System.out.println(r.partition() + "@" + r.offset() + "\t" + r.keyString() + "\t" + r.valueString());
                    if (++n >= max) {
                        break;
                    }
                }
                System.out.flush();
                c.commitSync();
            }
        }
    }

    private static void status(Args a) throws Exception {
        try (Admin admin = new Admin(a.bootstrap())) {
            for (StatusResponse s : admin.statusAll()) {
                System.out.println("broker " + s.nodeId() + (s.blocked().length > 0
                        ? "  (dropping traffic with " + Arrays.toString(s.blocked()) + ")" : ""));
                for (GroupStatus g : s.groups()) {
                    System.out.printf("  %-24s %-13s term=%-4d leader=%-3d last=%-8d commit=%d%n", g.name(), g.role(),
                            g.term(), g.leader(), g.lastIndex(), g.commitIndex());
                }
            }
        }
    }

    private static void chaos(Args a) throws Exception {
        int broker = Integer.parseInt(a.arg(0, "broker id"));
        int[] blocked = a.bool("heal") ? new int[0]
                : Arrays.stream(a.required("block").split(",")).map(String::trim).mapToInt(Integer::parseInt).toArray();
        try (Admin admin = new Admin(a.bootstrap())) {
            admin.chaos(broker, blocked);
        }
        System.out.println(blocked.length == 0 ? "broker " + broker + " healed"
                : "broker " + broker + " now drops traffic with " + Arrays.toString(blocked));
    }

    private static void bench(Args a) throws Exception {
        String topic = a.get("topic", "bench-" + System.currentTimeMillis());
        int partitions = a.integer("partitions", 3);
        int records = a.integer("records", 200_000);
        int size = Math.max(8, a.integer("size", 100));
        int inflight = a.integer("inflight", 0); // 0 = unbounded
        String bootstrap = a.bootstrap();
        try (Admin admin = new Admin(bootstrap)) {
            try {
                admin.createTopic(topic, partitions);
            } catch (ForgeLogException e) {
                if (e.error != Errors.TOPIC_ALREADY_EXISTS) {
                    throw e;
                }
            }
        }
        long[] latencies = new long[records];
        AtomicLong failed = new AtomicLong();
        java.util.concurrent.Semaphore window = new java.util.concurrent.Semaphore(inflight > 0 ? inflight : records);
        long start = System.nanoTime();
        try (Producer p = new Producer(bootstrap)) {
            for (int i = 0; i < records; i++) {
                byte[] payload = new byte[size];
                java.nio.ByteBuffer.wrap(payload).putLong(i); // a sequence number, checked by the consumer
                window.acquire();
                long sent = System.nanoTime();
                int idx = i;
                p.send(topic, i % partitions, null, payload).whenComplete((m, e) -> {
                    latencies[idx] = System.nanoTime() - sent;
                    if (e != null) {
                        failed.incrementAndGet();
                    }
                    window.release();
                });
            }
            p.flush();
        }
        double secs = (System.nanoTime() - start) / 1e9;
        Arrays.sort(latencies);
        System.out.printf("produce: %,d records x %d B in %.2f s = %,.0f records/s (%.1f MB/s), %d failed%n", records,
                size, secs, records / secs, records * (double) size / secs / 1e6, failed.get());
        System.out.printf("         ack latency (send -> quorum commit, %s in flight) p50 %.2f ms, p99 %.2f ms, max %.2f ms%n",
                inflight > 0 ? "<= " + inflight : "unbounded", latencies[records / 2] / 1e6,
                latencies[(int) (records * 0.99)] / 1e6, latencies[records - 1] / 1e6);

        java.util.BitSet seen = new java.util.BitSet(records);
        long got = 0;
        long duplicates = 0;
        long cstart = System.nanoTime();
        try (Consumer c = new Consumer(bootstrap, "bench-" + System.nanoTime(), List.of(topic))) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            long quietUntil = Long.MAX_VALUE;
            while (System.nanoTime() < Math.min(deadline, quietUntil)) {
                for (ConsumerRecord r : c.poll(Duration.ofMillis(200))) {
                    int seq = (int) java.nio.ByteBuffer.wrap(r.value()).getLong();
                    got++;
                    if (seq >= 0 && seq < records && seen.get(seq)) {
                        duplicates++;
                    }
                    if (seq >= 0 && seq < records) {
                        seen.set(seq);
                    }
                }
                if (quietUntil == Long.MAX_VALUE && seen.cardinality() >= records - failed.get()) {
                    quietUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(1); // linger to catch duplicates
                }
            }
        }
        double csecs = (System.nanoTime() - cstart) / 1e9;
        System.out.printf("consume: %,d records in %.2f s (incl. group join and a 1 s linger)%n", got, csecs);
        System.out.printf("verify:  %,d distinct of %,d sent, %,d missing, %,d duplicates%n", seen.cardinality(),
                records, records - seen.cardinality(), duplicates);
    }
}
