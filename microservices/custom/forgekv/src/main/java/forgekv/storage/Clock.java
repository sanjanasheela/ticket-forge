package forgekv.storage;

/** Wall-clock time. */
public final class Clock {
    private Clock() {}

    /** Current UNIX time in milliseconds. */
    public static long nowMs() {
        return System.currentTimeMillis();
    }
}
