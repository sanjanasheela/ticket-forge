package forgekv.resp;

import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** A single RESP2 value. */
public sealed interface Frame {
    /** {@code +OK\r\n} */
    record Simple(String value) implements Frame {}

    /** {@code -ERR message\r\n} */
    record Error(String message) implements Frame {}

    /** {@code :42\r\n} */
    record Int(long value) implements Frame {}

    /** {@code $5\r\nhello\r\n} */
    record Bulk(Bytes value) implements Frame {}

    /** {@code $-1\r\n} (null bulk string) */
    record Null() implements Frame {}

    /** {@code *-1\r\n} (null array) */
    record NullArray() implements Frame {}

    /** {@code *2\r\n...} */
    record Array(List<Frame> items) implements Frame {}

    Frame OK = new Simple("OK");
    Frame NULL = new Null();
    Frame NULL_ARRAY = new NullArray();
    Frame EMPTY_ARRAY = new Array(List.of());

    static Frame simple(String s) {
        return new Simple(s);
    }

    static Frame error(String s) {
        return new Error(s);
    }

    static Frame integer(long n) {
        return new Int(n);
    }

    static Frame bulk(Bytes b) {
        return new Bulk(b);
    }

    static Frame bulk(String s) {
        return new Bulk(Bytes.of(s));
    }

    static Frame array(List<Frame> items) {
        return new Array(items);
    }

    static Frame array(Frame... items) {
        return new Array(List.of(items));
    }

    /** Serialize this frame into {@code out}. */
    default void encode(ByteBuf out) {
        switch (this) {
            case Simple s -> out.put((byte) '+').put(s.value().getBytes(StandardCharsets.UTF_8)).crlf();
            case Error e -> out.put((byte) '-').put(e.message().getBytes(StandardCharsets.UTF_8)).crlf();
            case Int i -> out.put((byte) ':').putLong(i.value()).crlf();
            case Bulk b -> {
                Bytes v = b.value();
                out.ensureWritable(v.length() + 16);
                out.put((byte) '$').putLong(v.length()).crlf().put(v).crlf();
            }
            case Null n -> out.putAscii("$-1\r\n");
            case NullArray n -> out.putAscii("*-1\r\n");
            case Array a -> {
                out.put((byte) '*').putLong(a.items().size()).crlf();
                for (Frame item : a.items()) {
                    item.encode(out);
                }
            }
        }
    }

    /** Convenience: encode into a fresh buffer. */
    default Bytes toBytes() {
        ByteBuf out = new ByteBuf(64);
        encode(out);
        return out.toBytes();
    }

    default boolean isError() {
        return this instanceof Error;
    }
}
