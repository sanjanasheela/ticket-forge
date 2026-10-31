package forgelog;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * The wire format. Every message is {@code int length} then {@code length} bytes. A request starts with an
 * opcode byte; a response starts with a status byte (0 ok, 1 error followed by a message). Strings and byte
 * arrays are an {@code int} length (-1 for null) and the bytes.
 */
final class Wire {

    static final byte PRODUCE = 1, FETCH = 2, COMMIT = 3, COMMITTED = 4, JOIN = 5, HEARTBEAT = 6, LEAVE = 7, META = 8;

    private Wire() {
    }

    static void writeBytes(DataOutputStream d, byte[] b) throws IOException {
        if (b == null) {
            d.writeInt(-1);
        } else {
            d.writeInt(b.length);
            d.write(b);
        }
    }

    static byte[] readBytes(DataInputStream d) throws IOException {
        int n = d.readInt();
        if (n < 0) {
            return null;
        }
        byte[] b = new byte[n];
        d.readFully(b);
        return b;
    }

    static void writeString(DataOutputStream d, String s) throws IOException {
        writeBytes(d, s == null ? null : s.getBytes(StandardCharsets.UTF_8));
    }

    static String readString(DataInputStream d) throws IOException {
        byte[] b = readBytes(d);
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    /** Builds a message body, then sends it with its length prefix in one write. */
    static final class Out {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
        final DataOutputStream data = new DataOutputStream(bytes);

        void send(DataOutputStream socket) throws IOException {
            data.flush();
            socket.writeInt(bytes.size());
            bytes.writeTo(socket);
            socket.flush();
        }
    }

    static DataInputStream readFrame(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > 64 * 1024 * 1024) {
            throw new IOException("bad frame length " + n);
        }
        byte[] b = new byte[n];
        in.readFully(b);
        return new DataInputStream(new java.io.ByteArrayInputStream(b));
    }
}
