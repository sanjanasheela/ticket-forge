package forgelog;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Reads one topic as a member of a group. It joins, gets its share of the partitions and runs one thread per
 * partition: fetch (long poll), hand each record to the handler, commit the next offset after the batch.
 *
 * <p>If the handler throws, the same record is retried every second and the partition waits (nothing is
 * skipped; the callers' events must not be lost). A heartbeat thread keeps membership alive and, when the
 * group changes or the broker restarts, stops the partition threads and joins again. Delivery is at least once:
 * a rebalance or crash can replay the records after the last commit.
 */
public final class GroupConsumer implements Closeable {

    @FunctionalInterface
    public interface Handler {
        void handle(Rec record) throws Exception;
    }

    private final String bootstrap;
    private final String group;
    private final String topic;
    private final Handler handler;
    private final String member = UUID.randomUUID().toString();
    private final Thread coordinatorThread;
    private volatile boolean closed;

    public GroupConsumer(String bootstrap, String group, String topic, Handler handler) {
        this.bootstrap = bootstrap;
        this.group = group;
        this.topic = topic;
        this.handler = handler;
        this.coordinatorThread = Thread.ofPlatform().name("forgelog-group-" + group + "-" + topic).daemon(true).start(this::run);
    }

    private void run() {
        try (Client control = new Client(bootstrap)) {
            while (!closed) {
                List<Thread> workers = new ArrayList<>();
                Stop stop = new Stop();
                try {
                    int[] joined = control.join(group, topic, member);
                    int generation = joined[0];
                    for (int i = 1; i < joined.length; i++) {
                        int partition = joined[i];
                        workers.add(Thread.ofVirtual().name("forgelog-" + topic + "-" + partition).start(() -> consume(partition, stop)));
                    }
                    while (!closed) {
                        Thread.sleep(2_000);
                        if (control.heartbeat(group, topic, member) != generation) {
                            break;      // group changed (or we were dropped): re-join for a new assignment
                        }
                    }
                } catch (IOException e) {
                    sleepQuietly(1_000);   // broker down: keep trying
                } catch (InterruptedException e) {
                    return;
                } finally {
                    stop.stop = true;
                    workers.forEach(Thread::interrupt);
                    for (Thread w : workers) {
                        try {
                            w.join(12_000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
            try {
                control.leave(group, topic, member);
            } catch (IOException ignored) {
                // the session timeout will remove us
            }
        }
    }

    private static final class Stop {
        volatile boolean stop;
    }

    private void consume(int partition, Stop stop) {
        try (Client c = new Client(bootstrap)) {
            long next = -1;
            while (!closed && !stop.stop) {
                try {
                    if (next < 0) {
                        long committed = c.committed(group, topic, partition);
                        next = committed < 0 ? 0 : committed;       // a new group starts from the beginning
                    }
                    List<Rec> recs = c.fetch(topic, partition, next, 200, 500);
                    for (Rec r : recs) {
                        while (true) {
                            try {
                                handler.handle(r);
                                break;
                            } catch (Exception e) {
                                if (closed || stop.stop) {
                                    return;
                                }
                                sleepQuietly(1_000);
                            }
                        }
                    }
                    if (!recs.isEmpty()) {
                        next = recs.get(recs.size() - 1).offset() + 1;
                        c.commit(group, topic, partition, next);
                    }
                } catch (IOException e) {
                    next = -1;      // reconnect and re-read the committed offset
                    sleepQuietly(1_000);
                }
            }
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        closed = true;
        coordinatorThread.interrupt();
    }
}
