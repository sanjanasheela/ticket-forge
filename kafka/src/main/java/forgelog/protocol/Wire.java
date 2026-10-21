package forgelog.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Big-endian binary encoding used by the ForgeLog protocol.
 *
 * <p>Every message travels in a frame: {@code int32 size} followed by
 * {@code size} bytes. A request is {@code int16 apiKey, int32 correlationId,
 * body}; a response is {@code int32 correlationId, body}. Strings are
 * {@code int16 length + UTF-8} and byte arrays {@code int32 length + bytes},
 * with length {@code -1} meaning null.
 */
public final class Wire {
    /** Largest frame either side accepts. */
    public static final int MAX_FRAME = 64 * 1024 * 1024;

    private Wire() {}

    /** A malformed message. */
    public static final class ProtocolException extends RuntimeException {
        public ProtocolException(String msg) {
            super(msg);
        }
    }

    public static final class Writer {
        private ByteBuffer b;

        public Writer() {
            this(256);
        }

        public Writer(int capacity) {
            b = ByteBuffer.allocate(capacity);
        }

        /** Room for {@code n} more bytes; returns the backing buffer positioned at the end. */
        public ByteBuffer ensure(int n) {
            if (b.remaining() < n) {
                ByteBuffer next = ByteBuffer.allocate(Math.max(b.capacity() * 2, b.position() + n));
                b.flip();
                next.put(b);
                b = next;
            }
            return b;
        }

        public Writer i8(int v) {
            ensure(1).put((byte) v);
            return this;
        }

        public Writer i16(int v) {
            ensure(2).putShort((short) v);
            return this;
        }

        public Writer i32(int v) {
            ensure(4).putInt(v);
            return this;
        }

        public Writer i64(long v) {
            ensure(8).putLong(v);
            return this;
        }

        public Writer bool(boolean v) {
            return i8(v ? 1 : 0);
        }

        public Writer str(String s) {
            if (s == null) {
                return i16(-1);
            }
            byte[] u = s.getBytes(StandardCharsets.UTF_8);
            if (u.length > Short.MAX_VALUE) {
                throw new IllegalArgumentException("string too long");
            }
            i16(u.length);
            ensure(u.length).put(u);
            return this;
        }

        public Writer bytes(byte[] v) {
            if (v == null) {
                return i32(-1);
            }
            i32(v.length);
            ensure(v.length).put(v);
            return this;
        }

        public Writer raw(ByteBuffer src) {
            ensure(src.remaining()).put(src.duplicate());
            return this;
        }

        public Writer strings(List<String> l) {
            i32(l.size());
            for (String s : l) {
                str(s);
            }
            return this;
        }

        public Writer ints(int[] v) {
            i32(v.length);
            for (int x : v) {
                i32(x);
            }
            return this;
        }

        public int position() {
            return b.position();
        }

        public void setI32(int pos, int v) {
            b.putInt(pos, v);
        }

        /** The written bytes (a read-only view). */
        public ByteBuffer toBuffer() {
            return b.duplicate().flip();
        }
    }

    public static final class Reader {
        private final ByteBuffer b;

        public Reader(ByteBuffer b) {
            this.b = b;
        }

        public ByteBuffer buffer() {
            return b;
        }

        private void need(int n) {
            if (b.remaining() < n) {
                throw new ProtocolException("message truncated");
            }
        }

        public byte i8() {
            need(1);
            return b.get();
        }

        public short i16() {
            need(2);
            return b.getShort();
        }

        public int i32() {
            need(4);
            return b.getInt();
        }

        public long i64() {
            need(8);
            return b.getLong();
        }

        public boolean bool() {
            return i8() != 0;
        }

        public String str() {
            int n = i16();
            if (n == -1) {
                return null;
            }
            if (n < 0) {
                throw new ProtocolException("bad string length " + n);
            }
            need(n);
            byte[] u = new byte[n];
            b.get(u);
            return new String(u, StandardCharsets.UTF_8);
        }

        public byte[] bytes() {
            int n = i32();
            if (n == -1) {
                return null;
            }
            if (n < 0) {
                throw new ProtocolException("bad byte array length " + n);
            }
            need(n);
            byte[] v = new byte[n];
            b.get(v);
            return v;
        }

        /** The next {@code n} bytes as a view, advancing past them. */
        public ByteBuffer slice(int n) {
            if (n < 0) {
                throw new ProtocolException("bad length " + n);
            }
            need(n);
            ByteBuffer s = b.slice(b.position(), n);
            b.position(b.position() + n);
            return s;
        }

        /** A count prefix, sanity-checked against the bytes left. */
        public int count() {
            int n = i32();
            if (n < 0 || n > b.remaining()) {
                throw new ProtocolException("bad element count " + n);
            }
            return n;
        }

        public <T> List<T> list(Function<Reader, T> item) {
            int n = count();
            List<T> out = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                out.add(item.apply(this));
            }
            return out;
        }

        public List<String> strings() {
            return list(Reader::str);
        }

        public int[] ints() {
            int n = count();
            int[] v = new int[n];
            for (int i = 0; i < n; i++) {
                v[i] = i32();
            }
            return v;
        }

        public int remaining() {
            return b.remaining();
        }
    }
}
