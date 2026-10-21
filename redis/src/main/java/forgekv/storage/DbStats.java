package forgekv.storage;

/** Counters exposed through {@code INFO}. */
public record DbStats(long expiredKeys, long keyspaceHits, long keyspaceMisses) {
    public static final DbStats ZERO = new DbStats(0, 0, 0);
}
