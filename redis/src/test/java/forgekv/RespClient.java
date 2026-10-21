package forgekv;

import forgekv.resp.Frame;
import forgekv.resp.ProtocolException;
import forgekv.resp.Resp;
import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A tiny blocking RESP2 client for tests (the counterpart of the Rust `redis` crate). */
public final class RespClient implements AutoCloseable {
    /** An error reply. */
    public static final class RespError extends RuntimeException {
        public RespError(String msg) {
            super(msg);
        }
    }

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final ByteBuf buf = new ByteBuf(64 * 1024);

    public RespClient(int port) {
        this(new InetSocketAddress("127.0.0.1", port));
    }

    public RespClient(InetSocketAddress addr) {
        try {
            socket = new Socket();
            socket.connect(addr, 5000);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(30_000);
            in = socket.getInputStream();
            out = socket.getOutputStream();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void setTimeout(int ms) {
        try {
            socket.setSoTimeout(ms);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Bytes arg(Object o) {
        return switch (o) {
            case Bytes b -> b;
            case byte[] b -> Bytes.wrap(b.clone());
            case String s -> Bytes.of(s);
            default -> Bytes.of(String.valueOf(o));
        };
    }

    public static Bytes encode(Object... args) {
        ByteBuf b = new ByteBuf(64);
        b.put((byte) '*').putLong(args.length).crlf();
        for (Object o : args) {
            Bytes a = arg(o);
            b.put((byte) '$').putLong(a.length()).crlf().put(a).crlf();
        }
        return b.toBytes();
    }

    public void sendRaw(byte[] data) {
        try {
            out.write(data);
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void send(Object... args) {
        sendRaw(encode(args).array());
    }

    /** Read one reply (blocking). */
    public Frame read() {
        try {
            while (true) {
                Frame f = Resp.parse(buf);
                if (f != null) {
                    return f;
                }
                if (buf.readFrom(in, 64 * 1024) < 0) {
                    throw new UncheckedIOException(new EOFException("server closed the connection"));
                }
            }
        } catch (ProtocolException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Read up to one chunk of raw bytes. */
    public byte[] readRaw() {
        try {
            if (buf.isEmpty() && buf.readFrom(in, 4096) < 0) {
                return new byte[0];
            }
            return buf.take().array();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Frame call(Object... args) {
        send(args);
        return read();
    }

    /** Send every command in one write, then read every reply. */
    public List<Frame> pipeline(List<Object[]> cmds) {
        ByteBuf b = new ByteBuf(1024);
        for (Object[] c : cmds) {
            b.put(encode(c));
        }
        sendRaw(b.toByteArray());
        List<Frame> replies = new ArrayList<>(cmds.size());
        for (int i = 0; i < cmds.size(); i++) {
            replies.add(read());
        }
        return replies;
    }

    // ---- typed helpers (throw RespError on error replies) ----

    public static Frame check(Frame f) {
        if (f instanceof Frame.Error e) {
            throw new RespError(e.message());
        }
        return f;
    }

    public static String asString(Frame f) {
        return switch (check(f)) {
            case Frame.Bulk b -> b.value().toString();
            case Frame.Simple s -> s.value();
            case Frame.Int i -> Long.toString(i.value());
            case Frame.Null n -> null;
            default -> throw new IllegalStateException("not a string: " + f);
        };
    }

    public static Long asLong(Frame f) {
        return switch (check(f)) {
            case Frame.Int i -> i.value();
            case Frame.Bulk b -> Long.parseLong(b.value().toString());
            case Frame.Null n -> null;
            default -> throw new IllegalStateException("not an integer: " + f);
        };
    }

    public static List<String> asList(Frame f) {
        if (check(f) instanceof Frame.NullArray) {
            return null;
        }
        if (!(f instanceof Frame.Array a)) {
            throw new IllegalStateException("not an array: " + f);
        }
        List<String> out = new ArrayList<>();
        for (Frame i : a.items()) {
            out.add(asString(i));
        }
        return out;
    }

    public String str(Object... args) {
        return asString(call(args));
    }

    public Long integer(Object... args) {
        return asLong(call(args));
    }

    public List<String> list(Object... args) {
        return asList(call(args));
    }

    public byte[] bytes(Object... args) {
        Frame f = check(call(args));
        return f instanceof Frame.Bulk b ? b.value().array() : null;
    }

    public void ok(Object... args) {
        String r = str(args);
        if (!"OK".equals(r)) {
            throw new IllegalStateException("expected OK, got " + r);
        }
    }

    public Map<String, String> hgetall(String key) {
        List<String> l = list("HGETALL", key);
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < l.size(); i += 2) {
            m.put(l.get(i), l.get(i + 1));
        }
        return m;
    }

    /** One field from {@code INFO}. */
    public String info(String field) {
        String info = str("INFO");
        for (String line : info.split("\r\n")) {
            if (line.startsWith(field + ":")) {
                return line.substring(field.length() + 1).trim();
            }
        }
        return null;
    }

    public static String utf8(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException e) {
            // ignore
        }
    }
}
