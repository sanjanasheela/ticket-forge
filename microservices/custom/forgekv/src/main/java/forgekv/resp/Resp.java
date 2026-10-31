package forgekv.resp;

import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * RESP2 (REdis Serialization Protocol): incremental parser.
 *
 * <p>The parser is <em>incremental</em>: it never consumes bytes from the
 * buffer until a complete frame is available, so it works naturally with
 * partial TCP reads and with pipelined requests (many frames in one read).
 */
public final class Resp {
    /** Largest bulk string accepted (same as Redis' default {@code proto-max-bulk-len}). */
    public static final long MAX_BULK_LEN = 512L * 1024 * 1024;
    /** Largest number of elements accepted in a single array. */
    public static final long MAX_ARRAY_LEN = 1024 * 1024;
    /** Max nesting depth of arrays (protects against stack exhaustion). */
    private static final int MAX_DEPTH = 32;
    /** Max length of an inline (telnet-style) command line. */
    private static final int MAX_INLINE_LEN = 64 * 1024;

    private Resp() {}

    /**
     * Try to parse one complete frame from the front of {@code buf}.
     *
     * @return the frame (its bytes are consumed), or {@code null} if more data
     *     is needed ({@code buf} is untouched)
     * @throws ProtocolException on a protocol error
     */
    public static Frame parse(ByteBuf buf) throws ProtocolException {
        if (buf.isEmpty()) {
            return null;
        }
        Parser p = new Parser(buf.array(), buf.readIndex(), buf.writeIndex());
        Frame f = p.parseAt(0);
        if (f != null) {
            buf.skip(p.pos - buf.readIndex());
        }
        return f;
    }

    private static final class Parser {
        final byte[] b;
        final int end;
        int pos;

        Parser(byte[] b, int start, int end) {
            this.b = b;
            this.pos = start;
            this.end = end;
        }

        /** End index (exclusive, before CRLF) of the line at {@code pos}, or -1. */
        int lineEnd(int from) {
            for (int i = from; i + 1 < end; i++) {
                if (b[i] == '\r' && b[i + 1] == '\n') {
                    return i;
                }
            }
            return -1;
        }

        Frame parseAt(int depth) throws ProtocolException {
            if (depth > MAX_DEPTH) {
                throw new ProtocolException("nesting too deep");
            }
            if (pos >= end) {
                return null;
            }
            byte tag = b[pos];
            int bodyStart = pos + 1;
            switch (tag) {
                case '+', '-', ':', '$', '*' -> {}
                default -> {
                    if (depth == 0) {
                        return parseInline();
                    }
                    throw new ProtocolException("unexpected byte '" + (char) (tag & 0xFF) + "'");
                }
            }
            int le = lineEnd(bodyStart);
            if (le < 0) {
                return null;
            }
            int lineStart = bodyStart;
            pos = le + 2;
            switch (tag) {
                case '+':
                    return new Frame.Simple(new String(b, lineStart, le - lineStart, StandardCharsets.UTF_8));
                case '-':
                    return new Frame.Error(new String(b, lineStart, le - lineStart, StandardCharsets.UTF_8));
                case ':':
                    return new Frame.Int(parseInt(lineStart, le));
                case '$': {
                    long len = parseInt(lineStart, le);
                    if (len == -1) {
                        return Frame.NULL;
                    }
                    if (len < 0 || len > MAX_BULK_LEN) {
                        throw new ProtocolException("invalid bulk length");
                    }
                    long dataEnd = pos + len;
                    if (end < dataEnd + 2) {
                        return null;
                    }
                    int de = (int) dataEnd;
                    if (b[de] != '\r' || b[de + 1] != '\n') {
                        throw new ProtocolException("bulk string not terminated by CRLF");
                    }
                    Bytes data = Bytes.copyOf(b, pos, (int) len);
                    pos = de + 2;
                    return new Frame.Bulk(data);
                }
                default: { // '*'
                    long n = parseInt(lineStart, le);
                    if (n == -1) {
                        return Frame.NULL_ARRAY;
                    }
                    if (n < 0 || n > MAX_ARRAY_LEN) {
                        throw new ProtocolException("invalid multibulk length");
                    }
                    List<Frame> items = new ArrayList<>((int) Math.min(n, 1024));
                    for (long i = 0; i < n; i++) {
                        Frame f = parseAt(depth + 1);
                        if (f == null) {
                            return null;
                        }
                        items.add(f);
                    }
                    return new Frame.Array(items);
                }
            }
        }

        /** Signed decimal integer, as Rust's {@code i64::from_str} accepts it. */
        long parseInt(int from, int to) throws ProtocolException {
            int i = from;
            if (i >= to) {
                throw new ProtocolException("invalid integer");
            }
            boolean neg = false;
            if (b[i] == '-' || b[i] == '+') {
                neg = b[i] == '-';
                i++;
                if (i >= to) {
                    throw new ProtocolException("invalid integer");
                }
            }
            long v = 0;
            for (; i < to; i++) {
                int d = b[i] - '0';
                if (d < 0 || d > 9) {
                    throw new ProtocolException("invalid integer");
                }
                // Accumulate negatively so Long.MIN_VALUE fits.
                if (v < (Long.MIN_VALUE + d) / 10) {
                    throw new ProtocolException("invalid integer");
                }
                v = v * 10 - d;
            }
            if (!neg) {
                if (v == Long.MIN_VALUE) {
                    throw new ProtocolException("invalid integer");
                }
                v = -v;
            }
            return v;
        }

        /** Inline commands, e.g. {@code PING\r\n} typed into telnet / netcat. */
        Frame parseInline() throws ProtocolException {
            int start = pos;
            int nl = -1;
            for (int i = start; i < end; i++) {
                if (b[i] == '\n') {
                    nl = i;
                    break;
                }
            }
            if (nl < 0) {
                if (end - start > MAX_INLINE_LEN) {
                    throw new ProtocolException("too big inline request");
                }
                return null;
            }
            int lineEnd = nl;
            if (lineEnd > start && b[lineEnd - 1] == '\r') {
                lineEnd--;
            }
            pos = nl + 1;
            List<Frame> parts = new ArrayList<>();
            int i = start;
            while (i < lineEnd) {
                while (i < lineEnd && isAsciiWhitespace(b[i])) {
                    i++;
                }
                int s = i;
                while (i < lineEnd && !isAsciiWhitespace(b[i])) {
                    i++;
                }
                if (i > s) {
                    parts.add(new Frame.Bulk(Bytes.copyOf(b, s, i - s)));
                }
            }
            return new Frame.Array(parts);
        }
    }

    private static boolean isAsciiWhitespace(byte c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == 0x0C;
    }
}
