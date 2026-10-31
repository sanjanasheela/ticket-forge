package forgekv.server;

import forgekv.util.Bytes;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A bounded, multi-producer queue of encoded bytes destined for one
 * connection (a Pub/Sub subscriber or a replica stream), the counterpart of a
 * bounded tokio {@code mpsc} channel.
 *
 * <p>Producers on any thread {@link #offer} without blocking; the consumer
 * lives on its shard's thread and is woken through {@code owner}. Wake-ups
 * are coalesced: at most one is pending at a time.
 */
public final class Outbox {
    public enum Offer { OK, FULL, CLOSED }

    private final ArrayBlockingQueue<Bytes> queue;
    private final Executor owner;
    private final Runnable onReady;
    private final AtomicBoolean scheduled = new AtomicBoolean();
    private volatile boolean closed;

    /**
     * @param owner runs {@code onReady} on the consumer's thread
     * @param onReady drains the queue; called after items arrive or on close
     */
    public Outbox(int capacity, Executor owner, Runnable onReady) {
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.owner = owner;
        this.onReady = onReady;
    }

    public Offer offer(Bytes b) {
        if (closed) {
            return Offer.CLOSED;
        }
        if (!queue.offer(b)) {
            return Offer.FULL;
        }
        wake();
        return Offer.OK;
    }

    /** Next queued item, or {@code null}. */
    public Bytes poll() {
        return queue.poll();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /** No more items will be accepted (the producer side went away). */
    public void close() {
        closed = true;
        wake();
    }

    public boolean isClosed() {
        return closed;
    }

    /** Ask the consumer to look at the queue again. */
    public void wake() {
        if (scheduled.compareAndSet(false, true)) {
            owner.execute(() -> {
                scheduled.set(false);
                onReady.run();
            });
        }
    }
}
