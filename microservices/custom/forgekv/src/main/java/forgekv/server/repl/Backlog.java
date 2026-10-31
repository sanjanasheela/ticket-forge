package forgekv.server.repl;

import forgekv.util.Bytes;

/**
 * The last {@code cap} bytes of the replication stream, as a ring buffer.
 * Not thread-safe (guarded by the hub lock).
 */
final class Backlog {
    private final byte[] buf;
    final int cap;
    private int head;
    private int len;
    /** Stream offset just past the last byte ever appended. */
    long end;

    Backlog(int cap) {
        this.cap = Math.max(1, cap);
        this.buf = new byte[this.cap];
    }

    long start() {
        return end - len;
    }

    int histlen() {
        return len;
    }

    void push(byte[] data) {
        int n = data.length;
        end += n;
        if (n >= cap) {
            System.arraycopy(data, n - cap, buf, 0, cap);
            head = 0;
            len = cap;
            return;
        }
        int tail = (int) ((head + (long) len) % cap);
        int first = Math.min(n, cap - tail);
        System.arraycopy(data, 0, buf, tail, first);
        System.arraycopy(data, first, buf, 0, n - first);
        len += n;
        if (len > cap) {
            head = (head + (len - cap)) % cap;
            len = cap;
        }
    }

    /** Everything from {@code offset} on, or {@code null} if the backlog no longer covers it. */
    Bytes since(long offset) {
        if (offset < start() || offset > end) {
            return null;
        }
        int skip = (int) (offset - start());
        int count = len - skip;
        byte[] out = new byte[count];
        int from = (head + skip) % cap;
        int first = Math.min(count, cap - from);
        System.arraycopy(buf, from, out, 0, first);
        System.arraycopy(buf, 0, out, first, count - first);
        return Bytes.wrap(out);
    }
}
