package forgekv.server.repl;

import forgekv.resp.Frame;
import forgekv.resp.ProtocolException;
import forgekv.resp.Resp;
import forgekv.server.Batch;
import forgekv.server.Mode;
import forgekv.server.Shard;
import forgekv.server.ShardMsg;
import forgekv.server.aof.AofLog;
import forgekv.storage.Clock;
import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import forgekv.util.Log;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Follower side of replication: one thread holding a blocking connection to
 * the leader. Received commands are applied in batches on {@code shard}'s
 * thread, through the same {@link Batch} routing clients use, so a replica
 * may run a different shard count than its leader.
 */
final class ReplicaLink extends Thread {
    private final Shard shard;
    private final Replication repl;
    private final String host;
    private final int port;
    private volatile boolean stopped;
    private volatile Socket socket;

    ReplicaLink(Shard shard, String host, int port) {
        super("forgekv-replica-link");
        setDaemon(true);
        this.shard = shard;
        this.repl = shard.router.repl;
        this.host = host;
        this.port = port;
    }

    void shutdown() {
        stopped = true;
        Socket s = socket;
        if (s != null) {
            try {
                s.close();
            } catch (IOException e) {
                // ignore
            }
        }
        interrupt();
    }

    @Override
    public void run() {
        long backoff = 100;
        while (!stopped) {
            try {
                session();
                if (!stopped) {
                    Log.info("leader " + host + ":" + port + " closed the replication link");
                }
            } catch (IOException | RuntimeException e) {
                if (!stopped) {
                    Log.warn("replication link to " + host + ":" + port + ": " + e);
                }
            } catch (InterruptedException e) {
                // shutdown() interrupts; the loop condition decides.
            } finally {
                Socket s = socket;
                if (s != null) {
                    try {
                        s.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
            if (stopped) {
                return;
            }
            if (repl.linkUp.getAndSet(false)) {
                backoff = 100;
            }
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException e) {
                // re-check stopped
            }
            backoff = Math.min(backoff * 2, 2000);
        }
    }

    private static Bytes command(String... parts) {
        ByteBuf out = new ByteBuf(64);
        Bytes[] b = new Bytes[parts.length];
        for (int i = 0; i < parts.length; i++) {
            b[i] = Bytes.of(parts[i]);
        }
        AofLog.encodeCommand(out, b);
        return out.toBytes();
    }

    private static void send(OutputStream out, Bytes b) throws IOException {
        out.write(b.array());
        out.flush();
    }

    /** Read one {@code \r\n}-terminated line (without the terminator). */
    private static String readLine(InputStream in, ByteBuf buf) throws IOException {
        while (true) {
            int pos = buf.indexOfCrlf(buf.readIndex());
            if (pos >= 0) {
                String line = new String(buf.array(), buf.readIndex(), pos - buf.readIndex(), StandardCharsets.UTF_8);
                buf.skip(pos + 2 - buf.readIndex());
                return line;
            }
            if (buf.readFrom(in, 16 * 1024) < 0) {
                throw new EOFException("leader closed the connection");
            }
        }
    }

    private static String expectOk(InputStream in, ByteBuf buf, String what) throws IOException {
        String line = readLine(in, buf);
        if (line.startsWith("-")) {
            throw new IOException(what + " refused: " + line);
        }
        return line;
    }

    private static Bytes[] frameArgs(Frame f) {
        if (!(f instanceof Frame.Array a) || a.items().isEmpty()) {
            return null;
        }
        Bytes[] args = new Bytes[a.items().size()];
        for (int i = 0; i < args.length; i++) {
            if (!(a.items().get(i) instanceof Frame.Bulk b)) {
                return null;
            }
            args[i] = b.value();
        }
        return args;
    }

    /**
     * Apply {@code cmds} (in order) on the shard thread and wait until done.
     * Returns the encoded replies.
     */
    private ByteBuf apply(List<Bytes[]> cmds) throws IOException, InterruptedException {
        CompletableFuture<ByteBuf> done = new CompletableFuture<>();
        boolean posted = shard.post(new ShardMsg.Run(sh -> {
            Batch b = new Batch(sh, Mode.REPLICATED);
            for (Bytes[] c : cmds) {
                b.command(c);
            }
            ByteBuf sink = new ByteBuf(256);
            b.finish(sink, () -> done.complete(sink));
        }));
        if (!posted) {
            throw new IOException("shard stopped");
        }
        while (true) {
            try {
                return done.get(100, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                if (stopped) {
                    throw new InterruptedException("link stopped");
                }
            } catch (ExecutionException e) {
                throw new IOException(e.getCause());
            }
        }
    }

    /** Apply a buffer of complete commands (a full-sync payload). */
    private void applyAll(ByteBuf data) throws IOException, InterruptedException {
        while (true) {
            List<Bytes[]> cmds = new ArrayList<>();
            while (cmds.size() < Replication.APPLY_BATCH) {
                Frame f;
                try {
                    f = Resp.parse(data);
                } catch (ProtocolException e) {
                    throw new IOException("bad snapshot: " + e.getMessage());
                }
                if (f == null) {
                    break;
                }
                Bytes[] args = frameArgs(f);
                if (args == null) {
                    throw new IOException("bad command in snapshot");
                }
                cmds.add(args);
            }
            if (cmds.isEmpty()) {
                return;
            }
            apply(cmds);
        }
    }

    private void session() throws IOException, InterruptedException {
        Socket s = new Socket();
        socket = s;
        if (stopped) {
            return;
        }
        s.connect(new InetSocketAddress(host, port), 5000);
        s.setTcpNoDelay(true);
        InputStream in = s.getInputStream();
        OutputStream out = s.getOutputStream();
        ByteBuf buf = new ByteBuf(64 * 1024);

        send(out, command("PING"));
        expectOk(in, buf, "PING");
        send(out, command("REPLCONF", "listening-port", Integer.toString(shard.router.port)));
        expectOk(in, buf, "REPLCONF");

        String cached = repl.cachedMasterReplid();
        String id = cached != null ? cached : "?";
        String off = cached != null ? Long.toString(repl.linkOffset.get()) : "-1";
        send(out, command("PSYNC", id, off));
        String reply = expectOk(in, buf, "PSYNC");
        String[] words = reply.trim().split("\\s+");
        switch (words[0]) {
            case "+FULLRESYNC" -> {
                String replid = words.length > 1 ? words[1] : "";
                long offset = 0;
                try {
                    offset = words.length > 2 ? Long.parseLong(words[2]) : 0;
                } catch (NumberFormatException e) {
                    // keep 0
                }
                String lenLine = readLine(in, buf);
                int len;
                try {
                    if (!lenLine.startsWith("$")) {
                        throw new NumberFormatException();
                    }
                    len = Integer.parseInt(lenLine.substring(1));
                } catch (NumberFormatException e) {
                    throw new IOException("bad snapshot header");
                }
                while (buf.readable() < len) {
                    if (buf.readFrom(in, Math.max(64 * 1024, len - buf.readable())) < 0) {
                        throw new EOFException("snapshot truncated");
                    }
                }
                ByteBuf payload = new ByteBuf(len + 16);
                payload.put(buf.array(), buf.readIndex(), len);
                buf.skip(len);
                Log.info("full sync from " + host + ":" + port + ": " + len + " bytes, offset " + offset);
                apply(List.<Bytes[]>of(new Bytes[] {Bytes.of("FLUSHALL")}));
                applyAll(payload);
                repl.linkOffset.set(offset);
                repl.setMasterReplid(this, replid);
            }
            case "+CONTINUE" -> Log.info("partial resync from " + host + ":" + port + " at offset " + off);
            default -> throw new IOException("unexpected PSYNC reply: " + reply);
        }
        if (stopped) {
            return;
        }
        repl.linkUp.set(true);
        repl.linkLastIo.set(Clock.nowMs());

        long nextAck = Clock.nowMs(); // like tokio's interval: the first tick fires at once
        while (!stopped) {
            // Apply every complete command received so far.
            List<Bytes[]> cmds = new ArrayList<>();
            long consumed = 0;
            boolean getack = false;
            while (cmds.size() < Replication.APPLY_BATCH) {
                int before = buf.readable();
                Frame f;
                try {
                    f = Resp.parse(buf);
                } catch (ProtocolException e) {
                    throw new IOException("bad replication stream: " + e.getMessage());
                }
                if (f == null) {
                    break;
                }
                consumed += before - buf.readable();
                Bytes[] args = frameArgs(f);
                if (args == null) {
                    continue;
                }
                if (args[0].equalsIgnoreCase("REPLCONF")) {
                    getack |= args.length > 1 && args[1].equalsIgnoreCase("GETACK");
                } else if (!args[0].equalsIgnoreCase("PING")) {
                    cmds.add(args);
                }
            }
            boolean more = cmds.size() >= Replication.APPLY_BATCH;
            if (!cmds.isEmpty()) {
                ByteBuf sink = apply(cmds);
                // A failing replicated command means the datasets have diverged.
                while (true) {
                    Frame r;
                    try {
                        r = Resp.parse(sink);
                    } catch (ProtocolException e) {
                        break;
                    }
                    if (r == null) {
                        break;
                    }
                    if (r instanceof Frame.Error e) {
                        Log.warn("replicated command failed: " + e.message());
                    }
                }
            }
            long offset = repl.linkOffset.addAndGet(consumed);
            if (getack) {
                send(out, command("REPLCONF", "ACK", Long.toString(offset)));
            }
            if (more) {
                continue;
            }
            long wait = nextAck - Clock.nowMs();
            if (wait <= 0) {
                send(out, command("REPLCONF", "ACK", Long.toString(repl.linkOffset.get())));
                nextAck = Clock.nowMs() + Replication.ACK_INTERVAL_MS;
                continue;
            }
            s.setSoTimeout((int) Math.max(1, wait));
            try {
                if (buf.readFrom(in, 64 * 1024) < 0) {
                    return;
                }
                repl.linkLastIo.set(Clock.nowMs());
            } catch (SocketTimeoutException e) {
                // ACK tick
            }
        }
    }
}
