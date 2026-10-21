package forgelog.broker;

import forgelog.util.Log;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;

/**
 * The outbound connection to one peer broker, carrying this broker's Raft
 * messages for every group (the peer answers over its own link to us).
 *
 * <p>Raft tolerates lost messages, so the link is simple: while it is down
 * messages are dropped, the queue is bounded, and it reconnects with backoff.
 * Heartbeats repair whatever was lost.
 */
final class PeerLink {
    private static final long MAX_QUEUED_BYTES = 64L << 20;

    final int peerId;
    private final InetSocketAddress addr;
    private final Selector selector;
    private SocketChannel ch;
    private SelectionKey key;
    private boolean connected;
    private final ArrayDeque<ByteBuffer> queue = new ArrayDeque<>();
    private long queuedBytes;
    private long reconnectAt;
    private long backoffMs = 50;

    PeerLink(int peerId, InetSocketAddress addr, Selector selector) {
        this.peerId = peerId;
        this.addr = addr;
        this.selector = selector;
    }

    boolean isConnected() {
        return connected;
    }

    void send(ByteBuffer frame) {
        if (!connected || queuedBytes + frame.remaining() > MAX_QUEUED_BYTES) {
            return; // dropped: Raft retries
        }
        queue.add(frame);
        queuedBytes += frame.remaining();
        try {
            write();
        } catch (IOException e) {
            fail(e);
        }
    }

    /** Called periodically: (re)connect when due. */
    void maintain(long now) {
        if (ch != null || now < reconnectAt) {
            return;
        }
        try {
            ch = SocketChannel.open();
            ch.configureBlocking(false);
            ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
            if (ch.connect(addr)) {
                key = ch.register(selector, SelectionKey.OP_READ, this);
                onConnected();
            } else {
                key = ch.register(selector, SelectionKey.OP_CONNECT, this);
            }
        } catch (IOException e) {
            fail(e);
        }
        reconnectAt = now + backoffMs;
    }

    private void onConnected() {
        connected = true;
        backoffMs = 50;
        Log.debug("connected to peer " + peerId + " at " + addr);
    }

    void ready() {
        try {
            if (key.isValid() && key.isConnectable()) {
                ch.finishConnect();
                key.interestOps(SelectionKey.OP_READ);
                onConnected();
            }
            if (key.isValid() && key.isReadable()) {
                // Peers never write on this link: data or EOF both mean it's broken.
                ByteBuffer sink = ByteBuffer.allocate(256);
                if (ch.read(sink) < 0) {
                    throw new IOException("peer closed the link");
                }
            }
            if (key.isValid() && key.isWritable()) {
                write();
            }
        } catch (IOException e) {
            fail(e);
        }
    }

    private void write() throws IOException {
        while (!queue.isEmpty()) {
            ByteBuffer b = queue.peek();
            int n = ch.write(b);
            queuedBytes -= n;
            if (b.hasRemaining()) {
                break;
            }
            queue.poll();
        }
        int ops = SelectionKey.OP_READ | (queue.isEmpty() ? 0 : SelectionKey.OP_WRITE);
        if (key.isValid() && key.interestOps() != ops) {
            key.interestOps(ops);
        }
    }

    private void fail(IOException e) {
        if (connected) {
            Log.info("link to peer " + peerId + " lost: " + e.getMessage());
        }
        close();
        backoffMs = Math.min(backoffMs * 2, 1000);
        reconnectAt = System.nanoTime() / 1_000_000 + backoffMs;
    }

    void close() {
        connected = false;
        queue.clear();
        queuedBytes = 0;
        if (key != null) {
            key.cancel();
        }
        if (ch != null) {
            try {
                ch.close();
            } catch (IOException e) {
                // ignore
            }
        }
        ch = null;
        key = null;
    }
}
