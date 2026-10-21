package forgelog.client;

import forgelog.protocol.Wire;
import forgelog.protocol.Wire.Reader;
import forgelog.protocol.Wire.Writer;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * A blocking connection to one broker with one request in flight at a time
 * (callers serialize on it). The response's correlation id is checked
 * against the request's.
 */
final class BrokerConnection implements Closeable {
    private final Socket socket;
    private final DataInputStream in;
    private final OutputStream out;
    private int nextCorrelation = 1;

    BrokerConnection(InetSocketAddress addr, int connectTimeoutMs) throws IOException {
        socket = new Socket();
        try {
            socket.connect(addr, connectTimeoutMs);
            socket.setTcpNoDelay(true);
            in = new DataInputStream(socket.getInputStream());
            out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
        } catch (IOException e) {
            socket.close();
            throw e;
        }
    }

    synchronized Reader call(short api, Consumer<Writer> body, int timeoutMs) throws IOException {
        int corr = nextCorrelation++;
        Writer w = new Writer(256);
        w.i32(0).i16(api).i32(corr);
        body.accept(w);
        ByteBuffer frame = w.toBuffer();
        frame.putInt(0, frame.remaining() - 4);
        out.write(frame.array(), frame.arrayOffset() + frame.position(), frame.remaining());
        out.flush();
        socket.setSoTimeout(Math.max(1, timeoutMs));
        int size = in.readInt();
        if (size < 4 || size > Wire.MAX_FRAME) {
            throw new IOException("bad response frame size " + size);
        }
        byte[] payload = new byte[size];
        in.readFully(payload);
        Reader r = new Reader(ByteBuffer.wrap(payload));
        int got = r.i32();
        if (got != corr) {
            throw new IOException("response for request " + got + ", expected " + corr);
        }
        return r;
    }

    boolean isClosed() {
        return socket.isClosed();
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
