package forgelog.broker;

import forgelog.log.Region;
import forgelog.protocol.Wire;
import forgelog.util.Log;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.List;

/**
 * An inbound connection (a client, or a peer broker sending Raft messages).
 * Reads length-prefixed frames and hands each to the broker; writes responses
 * from a queue of buffers and file regions, the latter with
 * {@code transferTo} (sendfile). Responses may complete out of order (long
 * polls); clients match them by correlation id.
 */
final class ClientConn {
    interface FrameHandler {
        void onFrame(ClientConn conn, ByteBuffer frame);

        void onClose(ClientConn conn);
    }

    private static final class RegionWrite {
        final Region region;
        long sent;

        RegionWrite(Region region) {
            this.region = region;
        }
    }

    private final SocketChannel ch;
    private final SelectionKey key;
    private final FrameHandler handler;
    private ByteBuffer in = ByteBuffer.allocate(64 * 1024);
    private final ArrayDeque<Object> out = new ArrayDeque<>();
    private boolean closed;

    ClientConn(SocketChannel ch, SelectionKey key, FrameHandler handler) {
        this.ch = ch;
        this.key = key;
        this.handler = handler;
    }

    boolean isClosed() {
        return closed;
    }

    String peer() {
        try {
            return String.valueOf(ch.getRemoteAddress());
        } catch (IOException e) {
            return "?";
        }
    }

    void ready() {
        try {
            if (key.isValid() && key.isWritable()) {
                write();
            }
            if (key.isValid() && key.isReadable()) {
                read();
            }
        } catch (IOException | Wire.ProtocolException e) {
            Log.debug("connection " + peer() + " closed: " + e);
            close();
        }
    }

    private void read() throws IOException {
        int n = ch.read(in);
        if (n < 0) {
            close();
            return;
        }
        in.flip();
        while (in.remaining() >= 4) {
            int size = in.getInt(in.position());
            if (size < 0 || size > Wire.MAX_FRAME) {
                throw new Wire.ProtocolException("bad frame size " + size);
            }
            if (in.remaining() < 4 + size) {
                if (in.capacity() < 4 + size) {
                    ByteBuffer bigger = ByteBuffer.allocate(4 + size);
                    bigger.put(in);
                    in = bigger;
                    return;
                }
                break;
            }
            ByteBuffer frame = in.slice(in.position() + 4, size);
            in.position(in.position() + 4 + size);
            ByteBuffer copy = ByteBuffer.allocate(size).put(frame).flip();
            handler.onFrame(this, copy);
            if (closed) {
                return;
            }
        }
        in.compact();
    }

    /** Queue a response made of {@link ByteBuffer}s and {@link Region}s. */
    void send(List<Object> parts) {
        if (closed) {
            return;
        }
        for (Object p : parts) {
            out.add(p instanceof Region r ? new RegionWrite(r) : p);
        }
        try {
            write();
        } catch (IOException e) {
            close();
        }
    }

    private void write() throws IOException {
        while (!out.isEmpty()) {
            Object head = out.peek();
            if (head instanceof ByteBuffer b) {
                ch.write(b);
                if (b.hasRemaining()) {
                    break;
                }
            } else {
                RegionWrite rw = (RegionWrite) head;
                long left = rw.region.length() - rw.sent;
                long n = rw.region.channel().transferTo(rw.region.position() + rw.sent, left, ch);
                rw.sent += n;
                if (rw.sent < rw.region.length()) {
                    if (n == 0 && left > 0 && !ch.isOpen()) {
                        throw new IOException("socket closed");
                    }
                    break;
                }
            }
            out.poll();
        }
        int ops = SelectionKey.OP_READ | (out.isEmpty() ? 0 : SelectionKey.OP_WRITE);
        if (key.isValid() && key.interestOps() != ops) {
            key.interestOps(ops);
        }
    }

    void close() {
        if (closed) {
            return;
        }
        closed = true;
        key.cancel();
        try {
            ch.close();
        } catch (IOException e) {
            // ignore
        }
        out.clear();
        handler.onClose(this);
    }
}
