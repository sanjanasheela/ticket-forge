package forgegate;

import java.util.concurrent.*;
import java.util.function.Supplier;

/** Request coalescing: concurrent calls with the same key share one execution of the loader. */
public final class Singleflight<V> {
    private final ConcurrentHashMap<String, CompletableFuture<V>> inflight = new ConcurrentHashMap<>();
    public final java.util.concurrent.atomic.LongAdder leaders = new java.util.concurrent.atomic.LongAdder();
    public final java.util.concurrent.atomic.LongAdder followers = new java.util.concurrent.atomic.LongAdder();

    public V call(String key, Supplier<V> loader) {
        CompletableFuture<V> mine = new CompletableFuture<>();
        CompletableFuture<V> existing = inflight.putIfAbsent(key, mine);
        if (existing != null) {
            followers.increment();
            return existing.join();
        }
        leaders.increment();
        try {
            V v = loader.get();
            mine.complete(v);
            return v;
        } catch (RuntimeException e) {
            mine.completeExceptionally(e);
            throw e;
        } finally {
            inflight.remove(key, mine);
        }
    }
}
