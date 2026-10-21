package forgelog.log;

import java.util.Arrays;

/**
 * Where each term starts in the log. Terms never decrease along a Raft log,
 * so the log is a short sequence of runs; {@code termAt} is a binary search
 * over the run starts and needs no disk access.
 */
final class TermIndex {
    private long[] starts = new long[16];
    private long[] terms = new long[16];
    private int n;

    /** Record that the entry at {@code offset} has {@code term} (called in log order). */
    void onAppend(long offset, long term) {
        if (n > 0 && terms[n - 1] == term) {
            return;
        }
        if (n > 0 && term < terms[n - 1]) {
            throw new IllegalStateException("term went backwards: " + term + " after " + terms[n - 1]);
        }
        if (n == starts.length) {
            starts = Arrays.copyOf(starts, n * 2);
            terms = Arrays.copyOf(terms, n * 2);
        }
        starts[n] = offset;
        terms[n] = term;
        n++;
    }

    private int run(long offset) {
        int lo = 0;
        int hi = n - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (starts[mid] <= offset) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    long termAt(long offset) {
        return n == 0 ? -1 : terms[run(offset)];
    }

    long startOf(long offset) {
        return n == 0 ? 0 : starts[run(offset)];
    }

    void truncateFrom(long offset) {
        while (n > 0 && starts[n - 1] >= offset) {
            n--;
        }
    }

    int runs() {
        return n;
    }
}
