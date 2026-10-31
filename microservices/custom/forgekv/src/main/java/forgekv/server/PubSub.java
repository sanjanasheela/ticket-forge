package forgekv.server;

import forgekv.resp.Frame;
import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pub/Sub: channel + pattern subscriptions with bounded per-client queues.
 *
 * <ul>
 *   <li>{@code PUBLISH} encodes the push frame <b>once</b> and shares the
 *       immutable {@link Bytes} with every matching subscriber's queue.
 *   <li>Each subscriber has a bounded queue ({@link #CLIENT_QUEUE}). A
 *       consumer that cannot keep up is disconnected instead of letting
 *       memory grow without bound: the same policy as Redis'
 *       {@code client-output-buffer-limit pubsub}.
 *   <li>Subscribers live on whichever shard thread accepted them, so the
 *       registry is one process-wide lock. Publishing never blocks on a
 *       subscriber, so the lock is only ever held for in-memory work.
 * </ul>
 */
public final class PubSub {
    /** Max undelivered messages queued per subscriber before it is dropped. */
    public static final int CLIENT_QUEUE = 4096;

    private static final Bytes MESSAGE = Bytes.of("message");
    private static final Bytes PMESSAGE = Bytes.of("pmessage");

    record Sub(long id, Outbox out, AtomicBoolean overflow) {}

    private final HashMap<Bytes, List<Sub>> channels = new HashMap<>();
    private final HashMap<Bytes, List<Sub>> patterns = new HashMap<>();

    /**
     * Deliver {@code msg} to every sub; returns how many accepted it. Subs
     * whose queue is full are flagged for disconnection and removed; closed
     * ones are dropped.
     */
    private static int deliver(List<Sub> subs, Bytes msg) {
        int delivered = 0;
        for (Iterator<Sub> it = subs.iterator(); it.hasNext();) {
            Sub s = it.next();
            switch (s.out.offer(msg)) {
                case OK -> delivered++;
                case FULL -> {
                    s.overflow.set(true);
                    s.out.wake();
                    it.remove();
                }
                case CLOSED -> it.remove();
            }
        }
        return delivered;
    }

    private static void removeSub(Map<Bytes, List<Sub>> map, Bytes key, long id) {
        List<Sub> v = map.get(key);
        if (v != null) {
            v.removeIf(s -> s.id == id);
            if (v.isEmpty()) {
                map.remove(key);
            }
        }
    }

    /** Publish {@code payload} on {@code channel}. Returns the number of receiving subscriptions. */
    public synchronized int publish(Bytes channel, Bytes payload) {
        int n = 0;
        List<Sub> subs = channels.get(channel);
        if (subs != null) {
            Frame frame = Frame.array(new Frame.Bulk(MESSAGE), new Frame.Bulk(channel), new Frame.Bulk(payload));
            n += deliver(subs, frame.toBytes());
            if (subs.isEmpty()) {
                channels.remove(channel);
            }
        }
        for (Iterator<Map.Entry<Bytes, List<Sub>>> it = patterns.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Bytes, List<Sub>> e = it.next();
            if (Glob.match(e.getKey().array(), channel.array())) {
                Frame frame = Frame.array(new Frame.Bulk(PMESSAGE), new Frame.Bulk(e.getKey()),
                        new Frame.Bulk(channel), new Frame.Bulk(payload));
                n += deliver(e.getValue(), frame.toBytes());
            }
            if (e.getValue().isEmpty()) {
                it.remove();
            }
        }
        return n;
    }

    /** {@code PUBSUB CHANNELS [pattern]} */
    public synchronized List<Bytes> activeChannels(Bytes pattern) {
        List<Bytes> out = new ArrayList<>();
        for (Bytes c : channels.keySet()) {
            if (pattern == null || Glob.match(pattern.array(), c.array())) {
                out.add(c);
            }
        }
        return out;
    }

    /** {@code PUBSUB NUMSUB channel} */
    public synchronized int numSubscribers(Bytes channel) {
        List<Sub> v = channels.get(channel);
        return v == null ? 0 : v.size();
    }

    /** {@code PUBSUB NUMPAT} */
    public synchronized int numPatterns() {
        return patterns.size();
    }

    private synchronized void add(boolean pattern, Bytes name, Sub sub) {
        (pattern ? patterns : channels).computeIfAbsent(name, k -> new ArrayList<>()).add(sub);
    }

    private synchronized void remove(boolean pattern, Bytes name, long id) {
        removeSub(pattern ? patterns : channels, name, id);
    }

    /**
     * Per-connection subscription state. {@link #close()} unregisters
     * everything; the connection calls it however it ends.
     */
    public static final class Client {
        private final PubSub registry;
        private final Sub sub;
        private final LinkedHashSet<Bytes> channels = new LinkedHashSet<>();
        private final LinkedHashSet<Bytes> patterns = new LinkedHashSet<>();

        /**
         * @param owner the subscriber's thread, which {@code onReady} runs on
         * @param onReady called when messages are queued (or on overflow)
         */
        public Client(PubSub registry, long id, Executor owner, Runnable onReady) {
            this.registry = registry;
            this.sub = new Sub(id, new Outbox(CLIENT_QUEUE, owner, onReady), new AtomicBoolean());
        }

        /** Total subscriptions; non-zero means the connection is in subscribed mode. */
        public int count() {
            return channels.size() + patterns.size();
        }

        /** True once the server dropped this client for being too slow. */
        public boolean overflowed() {
            return sub.overflow.get();
        }

        /** Next undelivered message, or {@code null}. */
        public Bytes poll() {
            return sub.out.poll();
        }

        private void reply(ByteBuf out, String kind, Bytes name) {
            Frame.array(Frame.bulk(kind), name == null ? Frame.NULL : new Frame.Bulk(name), Frame.integer(count()))
                    .encode(out);
        }

        public void subscribe(List<Bytes> names, ByteBuf out) {
            for (Bytes name : names) {
                if (channels.add(name)) {
                    registry.add(false, name, sub);
                }
                reply(out, "subscribe", name);
            }
        }

        public void psubscribe(List<Bytes> names, ByteBuf out) {
            for (Bytes name : names) {
                if (patterns.add(name)) {
                    registry.add(true, name, sub);
                }
                reply(out, "psubscribe", name);
            }
        }

        /** Empty {@code names} means "everything". */
        public void unsubscribe(List<Bytes> names, ByteBuf out) {
            unsub(false, channels, names, out, "unsubscribe");
        }

        public void punsubscribe(List<Bytes> names, ByteBuf out) {
            unsub(true, patterns, names, out, "punsubscribe");
        }

        private void unsub(boolean pattern, LinkedHashSet<Bytes> mine, List<Bytes> names, ByteBuf out, String kind) {
            List<Bytes> targets = names.isEmpty() ? new ArrayList<>(mine) : names;
            if (targets.isEmpty()) {
                reply(out, kind, null);
            }
            for (Bytes name : targets) {
                if (mine.remove(name)) {
                    registry.remove(pattern, name, sub.id);
                }
                reply(out, kind, name);
            }
        }

        public void close() {
            for (Bytes c : channels) {
                registry.remove(false, c, sub.id);
            }
            for (Bytes p : patterns) {
                registry.remove(true, p, sub.id);
            }
            channels.clear();
            patterns.clear();
            sub.out.close();
        }
    }
}
