package forgekv.storage;

/** A value plus its optional absolute expiry deadline (UNIX ms). */
public final class Entry {
    /** {@link #expiresAt()} of a key without a TTL. */
    public static final long NO_EXPIRY = -1;

    Value value;
    long expiresAt;

    public Entry(Value value, long expiresAt) {
        this.value = value;
        this.expiresAt = expiresAt;
    }

    public Value value() {
        return value;
    }

    /** The deadline, or {@link #NO_EXPIRY}. */
    public long expiresAt() {
        return expiresAt;
    }

    public boolean hasExpiry() {
        return expiresAt != NO_EXPIRY;
    }

    boolean isExpired(long now) {
        return expiresAt != NO_EXPIRY && expiresAt <= now;
    }
}
