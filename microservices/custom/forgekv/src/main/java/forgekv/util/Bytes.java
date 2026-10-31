package forgekv.util;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * An immutable, binary-safe byte string with value semantics (the Java
 * counterpart of {@code bytes::Bytes}). Keys, values, channel names and
 * command arguments are all {@code Bytes}.
 *
 * <p>The wrapped array is never mutated after construction, so instances can
 * be shared freely between threads.
 */
public final class Bytes implements Comparable<Bytes> {
    public static final Bytes EMPTY = new Bytes(new byte[0]);

    private final byte[] data;
    private int hash;

    private Bytes(byte[] data) {
        this.data = data;
    }

    /** Take ownership of {@code data}: the caller must not modify it afterwards. */
    public static Bytes wrap(byte[] data) {
        return data.length == 0 ? EMPTY : new Bytes(data);
    }

    public static Bytes copyOf(byte[] src, int off, int len) {
        return len == 0 ? EMPTY : new Bytes(Arrays.copyOfRange(src, off, off + len));
    }

    /** UTF-8 encoding of {@code s}. */
    public static Bytes of(String s) {
        return wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    public static Bytes ofLong(long n) {
        return wrap(Long.toString(n).getBytes(StandardCharsets.US_ASCII));
    }

    /** The backing array. Read-only by contract. */
    public byte[] array() {
        return data;
    }

    public int length() {
        return data.length;
    }

    public boolean isEmpty() {
        return data.length == 0;
    }

    public byte at(int i) {
        return data[i];
    }

    public Bytes concat(Bytes other) {
        byte[] out = Arrays.copyOf(data, data.length + other.data.length);
        System.arraycopy(other.data, 0, out, data.length, other.data.length);
        return wrap(out);
    }

    public boolean startsWith(byte[] prefix) {
        return data.length >= prefix.length && Arrays.equals(data, 0, prefix.length, prefix, 0, prefix.length);
    }

    /** ASCII case-insensitive comparison against an ASCII literal. */
    public boolean equalsIgnoreCase(String ascii) {
        if (ascii.length() != data.length) {
            return false;
        }
        for (int i = 0; i < data.length; i++) {
            if (lower(data[i]) != lower((byte) ascii.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public boolean equalsIgnoreCase(Bytes other) {
        if (other.data.length != data.length) {
            return false;
        }
        for (int i = 0; i < data.length; i++) {
            if (lower(data[i]) != lower(other.data[i])) {
                return false;
            }
        }
        return true;
    }

    /** ASCII lower-casing (non-ASCII bytes are left alone). */
    public Bytes toLowerAscii() {
        byte[] out = data.clone();
        for (int i = 0; i < out.length; i++) {
            out[i] = lower(out[i]);
        }
        return wrap(out);
    }

    /** ASCII upper-casing into a String with one char per byte. */
    public String toUpperAscii() {
        char[] out = new char[data.length];
        for (int i = 0; i < data.length; i++) {
            byte b = data[i];
            out[i] = (char) ((b >= 'a' && b <= 'z') ? b - 32 : b & 0xFF);
        }
        return new String(out);
    }

    private static byte lower(byte b) {
        return (b >= 'A' && b <= 'Z') ? (byte) (b + 32) : b;
    }

    /** Lossy UTF-8 decoding (invalid sequences become U+FFFD). */
    @Override
    public String toString() {
        return new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o instanceof Bytes b && Arrays.equals(data, b.data));
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0 && data.length > 0) {
            h = Arrays.hashCode(data);
            hash = h;
        }
        return h;
    }

    /** Unsigned lexicographic order, like Rust's {@code [u8]} ordering. */
    @Override
    public int compareTo(Bytes o) {
        return Arrays.compareUnsigned(data, o.data);
    }
}
