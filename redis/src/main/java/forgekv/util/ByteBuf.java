package forgekv.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A growable byte buffer with a read cursor (the Java counterpart of
 * {@code bytes::BytesMut}). Bytes are appended at the write index and
 * consumed from the read index; consumed space is reclaimed by compaction.
 *
 * <p>Not thread-safe: every buffer is owned by one thread.
 */
public final class ByteBuf {
    private static final byte[] DIGITS_MIN = Long.toString(Long.MIN_VALUE).getBytes(StandardCharsets.US_ASCII);

    private byte[] buf;
    private int r;
    private int w;

    public ByteBuf() {
        this(256);
    }

    public ByteBuf(int capacity) {
        buf = new byte[Math.max(16, capacity)];
    }

    public int readable() {
        return w - r;
    }

    public boolean isEmpty() {
        return w == r;
    }

    /** The backing array; readable bytes are {@code [readIndex, writeIndex)}. */
    public byte[] array() {
        return buf;
    }

    public int readIndex() {
        return r;
    }

    public int writeIndex() {
        return w;
    }

    public byte get(int absoluteIndex) {
        return buf[absoluteIndex];
    }

    /** Consume {@code n} readable bytes. */
    public void skip(int n) {
        r += n;
        if (r == w) {
            r = 0;
            w = 0;
        }
    }

    public void clear() {
        r = 0;
        w = 0;
    }

    /** Make room for {@code n} more bytes, compacting or growing as needed. */
    public void ensureWritable(int n) {
        if (buf.length - w >= n) {
            return;
        }
        int len = w - r;
        if (r > 0 && buf.length - len >= n && r >= buf.length / 2) {
            System.arraycopy(buf, r, buf, 0, len);
        } else {
            long want = Math.max((long) len + n, (long) buf.length * 2);
            if (want > Integer.MAX_VALUE - 16) {
                want = (long) len + n;
                if (want > Integer.MAX_VALUE - 16) {
                    throw new OutOfMemoryError("ByteBuf too large");
                }
            }
            byte[] next = new byte[(int) want];
            System.arraycopy(buf, r, next, 0, len);
            buf = next;
        }
        r = 0;
        w = len;
    }

    public ByteBuf put(byte b) {
        ensureWritable(1);
        buf[w++] = b;
        return this;
    }

    public ByteBuf put(byte[] src) {
        return put(src, 0, src.length);
    }

    public ByteBuf put(byte[] src, int off, int len) {
        ensureWritable(len);
        System.arraycopy(src, off, buf, w, len);
        w += len;
        return this;
    }

    public ByteBuf put(Bytes b) {
        return put(b.array());
    }

    /** Append the readable bytes of {@code other} (which is left unchanged). */
    public ByteBuf put(ByteBuf other) {
        return put(other.buf, other.r, other.readable());
    }

    /** Append an ASCII string (one byte per char). */
    public ByteBuf putAscii(String s) {
        int len = s.length();
        ensureWritable(len);
        for (int i = 0; i < len; i++) {
            buf[w++] = (byte) s.charAt(i);
        }
        return this;
    }

    /** Append the decimal digits of {@code n}. */
    public ByteBuf putLong(long n) {
        if (n == Long.MIN_VALUE) {
            return put(DIGITS_MIN);
        }
        ensureWritable(20);
        if (n < 0) {
            buf[w++] = '-';
            n = -n;
        }
        int start = w;
        do {
            buf[w++] = (byte) ('0' + (n % 10));
            n /= 10;
        } while (n != 0);
        for (int i = start, j = w - 1; i < j; i++, j--) {
            byte t = buf[i];
            buf[i] = buf[j];
            buf[j] = t;
        }
        return this;
    }

    public ByteBuf crlf() {
        ensureWritable(2);
        buf[w++] = '\r';
        buf[w++] = '\n';
        return this;
    }

    /** A copy of the readable bytes. */
    public Bytes toBytes() {
        return Bytes.copyOf(buf, r, w - r);
    }

    /** A copy of the readable bytes, which are then consumed. */
    public Bytes take() {
        Bytes b = toBytes();
        clear();
        return b;
    }

    public byte[] toByteArray() {
        return Arrays.copyOfRange(buf, r, w);
    }

    /** Index of the first {@code \r\n} at or after absolute index {@code from}, or -1. */
    public int indexOfCrlf(int from) {
        for (int i = Math.max(from, r); i + 1 < w; i++) {
            if (buf[i] == '\r' && buf[i + 1] == '\n') {
                return i;
            }
        }
        return -1;
    }

    /** One non-blocking read. Returns the channel's result (-1 on EOF). */
    public int readFrom(ReadableByteChannel ch, int minFree) throws IOException {
        ensureWritable(minFree);
        int n = ch.read(ByteBuffer.wrap(buf, w, buf.length - w));
        if (n > 0) {
            w += n;
        }
        return n;
    }

    /** One blocking read from a stream. Returns the stream's result (-1 on EOF). */
    public int readFrom(InputStream in, int minFree) throws IOException {
        ensureWritable(minFree);
        int n = in.read(buf, w, buf.length - w);
        if (n > 0) {
            w += n;
        }
        return n;
    }

    /** Write as much as the channel accepts; the written bytes are consumed. */
    public int writeTo(WritableByteChannel ch) throws IOException {
        int n = ch.write(ByteBuffer.wrap(buf, r, w - r));
        if (n > 0) {
            skip(n);
        }
        return n;
    }

    @Override
    public String toString() {
        return new String(buf, r, w - r, StandardCharsets.UTF_8);
    }
}
