package forgegate;

/**
 * Adaptive concurrency limit (Netflix "gradient2" flavour, Little's Law: inflight = throughput x latency).
 * Tracks a slowly moving long RTT and a fast short RTT. When short/long grows (backend queueing), the limit
 * shrinks by that ratio; otherwise it grows by sqrt(limit) per sample window.
 */
public final class GradientLimiter {
    private final int min, max;
    private final double tolerance, smoothing;
    private double limit;
    private double longRtt = -1;          // nanoseconds, EWMA
    private int inflight;
    private int samples;
    private double windowSum;
    private final int window;

    public GradientLimiter(int initial, int min, int max, double tolerance, double smoothing, int window) {
        this.limit = initial; this.min = min; this.max = max;
        this.tolerance = tolerance; this.smoothing = smoothing; this.window = window;
    }

    public GradientLimiter() { this(50, 5, 2000, 1.5, 0.2, 20); }

    public synchronized boolean tryAcquire() {
        if (inflight >= (int) limit) return false;
        inflight++;
        return true;
    }

    /** Release a permit and feed back the observed latency. Failures/timeouts count as "dropped": shrink. */
    public synchronized void release(long rttNanos, boolean dropped) {
        inflight--;
        if (dropped) { setLimit(limit * 0.9); return; }
        windowSum += rttNanos;
        if (++samples < window) return;
        double shortRtt = windowSum / samples;
        samples = 0; windowSum = 0;
        if (longRtt < 0) longRtt = shortRtt;
        else longRtt = longRtt * 0.95 + shortRtt * 0.05;     // slow baseline
        double gradient = Math.max(0.5, Math.min(1.0, tolerance * longRtt / shortRtt));
        double target = limit * gradient + Math.sqrt(limit);
        setLimit(limit * (1 - smoothing) + target * smoothing);
    }

    private void setLimit(double v) { limit = Math.max(min, Math.min(max, v)); }
    public synchronized int limit() { return (int) limit; }
    public synchronized int inflight() { return inflight; }
}
