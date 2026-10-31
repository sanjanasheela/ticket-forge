package forgegate;

/** Per-upstream breaker: opens after N consecutive failures, half-opens after a cool-down. */
public final class CircuitBreaker {
    private final int threshold; private final long coolNanos;
    private int failures; private long openedAt = -1; private boolean probing;

    public CircuitBreaker(int threshold, long coolMillis) { this.threshold = threshold; this.coolNanos = coolMillis * 1_000_000L; }

    public synchronized boolean allow() {
        if (openedAt < 0) return true;
        if (System.nanoTime() - openedAt >= coolNanos && !probing) { probing = true; return true; }
        return false;
    }
    public synchronized void success() { failures = 0; openedAt = -1; probing = false; }
    public synchronized void failure() {
        probing = false;
        if (++failures >= threshold) openedAt = System.nanoTime();
    }
    public synchronized boolean open() { return openedAt >= 0; }
}
