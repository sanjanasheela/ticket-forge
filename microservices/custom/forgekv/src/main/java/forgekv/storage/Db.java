package forgekv.storage;

import forgekv.util.Bytes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * The keyspace, with Redis-style expiry.
 *
 * <p>Expiry uses the same two mechanisms as Redis:
 * <ul>
 *   <li><b>Lazy</b>: every access checks the key's deadline and deletes it if passed.
 *   <li><b>Active</b>: {@link #activeExpireCycle} samples random keys that carry a
 *       TTL and deletes expired ones, repeating while &gt;25% of a sample was expired.
 * </ul>
 *
 * <p>To sample uniformly in O(1), keys with a TTL are tracked in an
 * {@link ExpireIndex} (a list + position map with swap-remove).
 *
 * <p>All deadlines are absolute UNIX timestamps in milliseconds, which makes
 * them trivially persistable (AOF) and replicable.
 *
 * <p>A replica runs in <b>passive expiry</b> mode ({@link #setPassiveExpiry}):
 * expired keys are hidden from reads but never deleted locally. The leader
 * decides when a key dies and replicates that as a {@code DEL}, so a replica
 * whose clock runs ahead can never diverge.
 *
 * <p>Single-owner (no internal locking): in the thread-per-core design each
 * shard owns exactly one {@code Db}.
 */
public final class Db {
    /** Keys sampled per active-expiry round (Redis: {@code ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP}). */
    private static final int ACTIVE_EXPIRE_SAMPLE = 20;
    /** Upper bound on rounds per cycle, bounding the time spent on one tick. */
    private static final int ACTIVE_EXPIRE_MAX_ROUNDS = 16;

    /** {@link #ttlMs} result: no such key. */
    public static final long TTL_MISSING = -2;
    /** {@link #ttlMs} result: the key has no expiry. */
    public static final long TTL_NONE = -1;

    private final HashMap<Bytes, Entry> map = new HashMap<>();
    private final ExpireIndex expires = new ExpireIndex();
    private long rng = Clock.nowMs() | 1;
    private long expiredKeys;
    private long hits;
    private long misses;
    /**
     * When enabled, keys removed by expiry are remembered until drained so the
     * persistence layer can log them (an expiry is a state change that replay
     * must reproduce, since replay runs on a frozen clock).
     */
    private boolean trackExpired;
    private ArrayList<Bytes> expiredLog = new ArrayList<>();
    /** Replica mode: hide expired keys instead of deleting them. */
    private boolean passiveExpiry;

    /**
     * Replica mode on/off. While on, lazy and active expiry never delete:
     * expired keys read as missing until the leader's {@code DEL} arrives.
     */
    public void setPassiveExpiry(boolean on) {
        passiveExpiry = on;
    }

    public boolean passiveExpiry() {
        return passiveExpiry;
    }

    /** Start/stop remembering expired keys (see {@link #drainExpired}). */
    public void setTrackExpired(boolean on) {
        trackExpired = on;
        if (!on) {
            expiredLog.clear();
        }
    }

    /** Take the keys that expired since the last call (empty unless tracking). */
    public List<Bytes> drainExpired() {
        if (expiredLog.isEmpty()) {
            return List.of();
        }
        List<Bytes> out = expiredLog;
        expiredLog = new ArrayList<>();
        return out;
    }

    /** Read an entry as stored, without expiry checks or hit/miss accounting. */
    public Entry peek(Bytes key) {
        return map.get(key);
    }

    /** Number of keys (including logically-expired keys not yet reclaimed). */
    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    /** Number of keys that carry a TTL. */
    public int expiresSize() {
        return expires.size();
    }

    public DbStats stats() {
        return new DbStats(expiredKeys, hits, misses);
    }

    public void clear() {
        map.clear();
        expires.clear();
    }

    /** Lazy expiry. Returns {@code true} if the key existed and was expired. */
    private boolean expireIfNeeded(Bytes key, long now) {
        Entry e = map.get(key);
        if (e == null || !e.isExpired(now)) {
            return false;
        }
        if (passiveExpiry) {
            return true;
        }
        remove(key);
        expiredKeys++;
        if (trackExpired) {
            expiredLog.add(key);
        }
        return true;
    }

    /** Look up a live entry, or {@code null}. */
    public Entry get(Bytes key, long now) {
        expireIfNeeded(key, now);
        Entry e = map.get(key);
        if (e != null && e.isExpired(now)) {
            e = null;
        }
        if (e != null) {
            hits++;
        } else {
            misses++;
        }
        return e;
    }

    /**
     * Access to a live value for in-place mutation, or {@code null}. The TTL
     * is not modifiable through this handle (use {@link #setExpiry}) so the
     * expire index stays consistent.
     */
    public Value getMut(Bytes key, long now) {
        expireIfNeeded(key, now);
        Entry e = map.get(key);
        return (e != null && !e.isExpired(now)) ? e.value : null;
    }

    public boolean contains(Bytes key, long now) {
        expireIfNeeded(key, now);
        Entry e = map.get(key);
        return e != null && !e.isExpired(now);
    }

    /** Insert/overwrite a key, replacing any previous TTL with {@code expiresAt}. */
    public void set(Bytes key, Value value, long expiresAt) {
        if (expiresAt != Entry.NO_EXPIRY) {
            expires.insert(key);
        } else {
            expires.remove(key);
        }
        map.put(key, new Entry(value, expiresAt));
    }

    /** Insert/overwrite a key, keeping its current TTL if it has one. */
    public void setKeepTtl(Bytes key, Value value, long now) {
        expireIfNeeded(key, now);
        Entry e = map.get(key);
        if (e != null) {
            e.value = value;
        } else {
            map.put(key, new Entry(value, Entry.NO_EXPIRY));
        }
    }

    /** Remove a key unconditionally (does not check expiry). */
    public Entry remove(Bytes key) {
        expires.remove(key);
        return map.remove(key);
    }

    /**
     * Set (a deadline) or clear ({@link Entry#NO_EXPIRY}) the deadline of a
     * live key. Returns {@code false} if the key does not exist.
     */
    public boolean setExpiry(Bytes key, long at, long now) {
        if (!contains(key, now)) {
            return false;
        }
        Entry e = map.get(key);
        e.expiresAt = at;
        if (at != Entry.NO_EXPIRY) {
            expires.insert(key);
        } else {
            expires.remove(key);
        }
        return true;
    }

    /** Remaining TTL in ms, {@link #TTL_MISSING} or {@link #TTL_NONE}. */
    public long ttlMs(Bytes key, long now) {
        expireIfNeeded(key, now);
        Entry e = map.get(key);
        if (e == null || e.isExpired(now)) {
            return TTL_MISSING;
        }
        if (!e.hasExpiry()) {
            return TTL_NONE;
        }
        return Math.max(0, e.expiresAt - now);
    }

    /** Delete every key for which {@code pred} holds; returns how many were removed. */
    public int removeWhere(Predicate<Bytes> pred) {
        List<Bytes> doomed = new ArrayList<>();
        for (Bytes k : map.keySet()) {
            if (pred.test(k)) {
                doomed.add(k);
            }
        }
        for (Bytes k : doomed) {
            remove(k);
        }
        return doomed.size();
    }

    /** Insert a complete entry (value + deadline), e.g. when re-partitioning. */
    public void insertEntry(Bytes key, Entry entry) {
        set(key, entry.value, entry.expiresAt);
    }

    /** Move every entry out, leaving the keyspace empty. */
    public List<Map.Entry<Bytes, Entry>> drain() {
        List<Map.Entry<Bytes, Entry>> out = new ArrayList<>(map.entrySet());
        map.clear();
        expires.clear();
        return out;
    }

    /** Visit all live keys (used by {@code KEYS}/{@code SCAN} and persistence). */
    public void forEachLive(long now, BiConsumer<Bytes, Entry> f) {
        for (Map.Entry<Bytes, Entry> e : map.entrySet()) {
            if (!e.getValue().isExpired(now)) {
                f.accept(e.getKey(), e.getValue());
            }
        }
    }

    private long nextRand() {
        // xorshift64: fast and good enough for sampling.
        long x = rng;
        x ^= x << 13;
        x ^= x >>> 7;
        x ^= x << 17;
        rng = x;
        return x;
    }

    /**
     * One active-expiry cycle (Redis' adaptive sampling algorithm).
     * Returns the number of keys reclaimed.
     */
    public int activeExpireCycle(long now) {
        if (passiveExpiry) {
            return 0;
        }
        int total = 0;
        for (int round = 0; round < ACTIVE_EXPIRE_MAX_ROUNDS; round++) {
            int n = expires.size();
            if (n == 0) {
                break;
            }
            int sample = Math.min(n, ACTIVE_EXPIRE_SAMPLE);
            int expired = 0;
            for (int i = 0; i < sample; i++) {
                int len = expires.size();
                if (len == 0) {
                    break;
                }
                int idx = (int) Long.remainderUnsigned(nextRand(), len);
                Bytes key = expires.keys.get(idx);
                Entry e = map.get(key);
                if (e == null || e.isExpired(now)) {
                    remove(key);
                    if (trackExpired) {
                        expiredLog.add(key);
                    }
                    expired++;
                }
            }
            total += expired;
            // Stop when <= 25% of the sample was stale: few expired keys remain.
            if (expired * 4 <= sample) {
                break;
            }
        }
        expiredKeys += total;
        return total;
    }
}
