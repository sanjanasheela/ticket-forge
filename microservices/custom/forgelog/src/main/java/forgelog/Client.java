package forgelog;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * One connection to the broker, used by one thread at a time (requests are synchronised). The socket is opened
 * on first use and reopened on the call after a failure, so a broker restart heals itself. A failed call throws
 * {@link IOException}; the caller decides whether to retry.
 */
public final class Client implements Closeable {

    /** A record to send. */
    public record Msg(String topic, String key, String type, String eventId, String value) {
    }

    private final String host;
    private final int port;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;

    public Client(String bootstrap) {
        int colon = bootstrap.lastIndexOf(':');
        this.host = bootstrap.substring(0, colon);
        this.port = Integer.parseInt(bootstrap.substring(colon + 1));
    }

    private interface Body {
        void write(DataOutputStream d) throws IOException;
    }

    private interface Reply<T> {
        T read(DataInputStream d) throws IOException;
    }

    private synchronized <T> T call(byte op, int timeoutMs, Body body, Reply<T> reply) throws IOException {
        try {
            if (socket == null) {
                socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.connect(new InetSocketAddress(host, port), 3_000);
                in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 65536));
                out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 65536));
            }
            socket.setSoTimeout(timeoutMs);
            Wire.Out req = new Wire.Out();
            req.data.writeByte(op);
            body.write(req.data);
            req.send(out);
            DataInputStream resp = Wire.readFrame(in);
            if (resp.readByte() != 0) {
                throw new IOException("broker error: " + Wire.readString(resp));
            }
            return reply.read(resp);
        } catch (IOException e) {
            closeQuietly();
            throw e;
        }
    }

    public int partitions(String topic) throws IOException {
        return call(Wire.META, 10_000, d -> Wire.writeString(d, topic), DataInputStream::readInt);
    }

    /** Sends records of ONE topic in one request; returns each record's offset. Same key -> same partition, in order. */
    public long[] produce(String topic, List<Msg> msgs) throws IOException {
        return call(Wire.PRODUCE, 15_000, d -> {
            Wire.writeString(d, topic);
            d.writeInt(msgs.size());
            for (Msg m : msgs) {
                Wire.writeBytes(d, m.key() == null ? null : m.key().getBytes(StandardCharsets.UTF_8));
                Wire.writeString(d, m.type());
                Wire.writeString(d, m.eventId());
                Wire.writeBytes(d, m.value().getBytes(StandardCharsets.UTF_8));
            }
        }, d -> {
            int n = d.readInt();
            long[] offsets = new long[n];
            for (int i = 0; i < n; i++) {
                d.readInt();
                offsets[i] = d.readLong();
            }
            return offsets;
        });
    }

    public List<Rec> fetch(String topic, int partition, long offset, int max, long waitMs) throws IOException {
        return call(Wire.FETCH, (int) waitMs + 10_000, d -> {
            Wire.writeString(d, topic);
            d.writeInt(partition);
            d.writeLong(offset);
            d.writeInt(max);
            d.writeLong(waitMs);
        }, d -> {
            int n = d.readInt();
            List<Rec> recs = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                recs.add(new Rec(d.readLong(), d.readLong(), Wire.readBytes(d), Wire.readString(d), Wire.readString(d), Wire.readBytes(d)));
            }
            return recs;
        });
    }

    public void commit(String group, String topic, int partition, long nextOffset) throws IOException {
        call(Wire.COMMIT, 10_000, d -> {
            Wire.writeString(d, group);
            Wire.writeString(d, topic);
            d.writeInt(partition);
            d.writeLong(nextOffset);
        }, d -> null);
    }

    /** The offset to resume from, or -1 if the group never committed. */
    public long committed(String group, String topic, int partition) throws IOException {
        return call(Wire.COMMITTED, 10_000, d -> {
            Wire.writeString(d, group);
            Wire.writeString(d, topic);
            d.writeInt(partition);
        }, DataInputStream::readLong);
    }

    /** Joins the group; returns {generation, assigned partitions...}. */
    public int[] join(String group, String topic, String member) throws IOException {
        return call(Wire.JOIN, 10_000, d -> member3(d, group, topic, member), d -> {
            int generation = d.readInt();
            int n = d.readInt();
            int[] r = new int[n + 1];
            r[0] = generation;
            for (int i = 0; i < n; i++) {
                r[i + 1] = d.readInt();
            }
            return r;
        });
    }

    public int heartbeat(String group, String topic, String member) throws IOException {
        return call(Wire.HEARTBEAT, 10_000, d -> member3(d, group, topic, member), DataInputStream::readInt);
    }

    public void leave(String group, String topic, String member) throws IOException {
        call(Wire.LEAVE, 5_000, d -> member3(d, group, topic, member), d -> null);
    }

    private static void member3(DataOutputStream d, String group, String topic, String member) throws IOException {
        Wire.writeString(d, group);
        Wire.writeString(d, topic);
        Wire.writeString(d, member);
    }

    private void closeQuietly() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
            // already broken
        }
        socket = null;
    }

    @Override
    public synchronized void close() {
        closeQuietly();
    }
}
