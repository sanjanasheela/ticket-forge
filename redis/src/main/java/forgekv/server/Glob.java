package forgekv.server;

/**
 * Redis-style glob pattern matching for {@code KEYS} / {@code SCAN MATCH}.
 *
 * <p>Supports {@code *}, {@code ?}, {@code [abc]}, {@code [a-z]},
 * {@code [^abc]} and {@code \} escapes. Matching is iterative with
 * single-star backtracking, so it is O(n·m) worst case and cannot blow the
 * stack or go exponential on patterns like {@code a*a*a*a*b}.
 */
public final class Glob {
    private Glob() {}

    public static boolean match(byte[] pattern, byte[] text) {
        int p = 0;
        int t = 0;
        // Position to resume from after the most recent '*'.
        int starP = -1;
        int starT = -1;

        while (t < text.length) {
            if (p < pattern.length) {
                byte c = pattern[p];
                if (c == '*') {
                    while (p < pattern.length && pattern[p] == '*') {
                        p++;
                    }
                    if (p == pattern.length) {
                        return true;
                    }
                    starP = p;
                    starT = t;
                    continue;
                } else if (c == '?') {
                    p++;
                    t++;
                    continue;
                } else if (c == '[') {
                    long r = matchClass(pattern, p, text[t]);
                    if (r >= 0) {
                        if ((r & 1) == 1) {
                            p = (int) (r >>> 1);
                            t++;
                            continue;
                        }
                    } else if (text[t] == '[') {
                        // Unterminated class: treat '[' literally.
                        p++;
                        t++;
                        continue;
                    }
                } else if (c == '\\' && p + 1 < pattern.length) {
                    if (pattern[p + 1] == text[t]) {
                        p += 2;
                        t++;
                        continue;
                    }
                } else if (c == text[t]) {
                    p++;
                    t++;
                    continue;
                }
            }
            // Mismatch: backtrack to the last star, consuming one more text byte.
            if (starP < 0) {
                return false;
            }
            p = starP;
            starT++;
            t = starT;
        }
        while (p < pattern.length && pattern[p] == '*') {
            p++;
        }
        return p == pattern.length;
    }

    /**
     * Match {@code c} against the class starting at {@code pattern[start] == '['}.
     * Returns {@code (indexAfterClass << 1) | matched}, or -1 if unterminated.
     */
    private static long matchClass(byte[] pattern, int start, byte c) {
        int i = start + 1;
        boolean negate = i < pattern.length && pattern[i] == '^';
        if (negate) {
            i++;
        }
        boolean matched = false;
        boolean first = true;
        while (i < pattern.length) {
            byte pc = pattern[i];
            if (pc == ']' && !first) {
                return ((long) (i + 1) << 1) | ((matched != negate) ? 1 : 0);
            } else if (pc == '\\' && i + 1 < pattern.length) {
                matched |= pattern[i + 1] == c;
                i += 2;
            } else if (i + 2 < pattern.length && pattern[i + 1] == '-' && pattern[i + 2] != ']') {
                int lo = pc & 0xFF;
                int hi = pattern[i + 2] & 0xFF;
                int a = Math.min(lo, hi);
                int b = Math.max(lo, hi);
                int cc = c & 0xFF;
                matched |= cc >= a && cc <= b;
                i += 3;
            } else {
                matched |= pc == c;
                i++;
            }
            first = false;
        }
        return -1;
    }
}
