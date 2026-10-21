package forgekv.server;

import forgekv.resp.Frame;
import forgekv.resp.ProtocolException;
import forgekv.resp.Resp;
import forgekv.server.cmd.CmdError;
import forgekv.server.cmd.Commands;
import forgekv.server.repl.Replication;
import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import forgekv.util.Log;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

/**
 * Per-connection read → parse → execute → write state machine.
 *
 * <p>Each read's worth of pipelined commands becomes one {@link Batch}: local
 * commands execute inline, remote ones go to their shards in one message per
 * shard, and all replies leave in a single {@code write}. While a batch is in
 * flight the connection stops reading, so replies always leave in request
 * order.
 *
 * <p>Connection-scoped commands are handled here rather than in the command
 * table: Pub/Sub (per-connection state), {@code PSYNC} (the connection becomes
 * a replication stream), and {@code INFO}/{@code BGREWRITEAOF}/{@code WAIT},
 * which have to wait on other shards. A connection with active subscriptions
 * is in <em>subscribed mode</em>: it also delivers queued messages, and only
 * a small set of commands is accepted.
 */
final class Connection {
    private static final int READ_CHUNK = 16 * 1024;

    private enum Flow { CONTINUE, CLOSE, ASYNC, PSYNC }

    private final Shard shard;
    private final SocketChannel ch;
    SelectionKey key;
    private final ByteBuf rbuf = new ByteBuf(READ_CHUNK);
    private final ByteBuf wbuf = new ByteBuf(READ_CHUNK);
    private PubSub.Client ps;
    /** {@code REPLCONF listening-port}, sent by replicas before {@code PSYNC}. */
    private int listeningPort = -1;

    /** A batch or async command is in flight: don't read or parse more. */
    private boolean busy;
    /** {@code wbuf} could not be fully written; waiting for OP_WRITE. */
    private boolean writePending;
    private boolean closeAfterFlush;
    private boolean closed;
    private boolean inProcess;
    private boolean again;
    /** Bytes arrived in {@code rbuf} that no {@link #process()} pass has looked at yet. */
    private boolean pendingInput;

    /** Set once this connection streams replication to a replica. */
    private Outbox replicaOut;
    private long replicaId;

    Connection(Shard shard, SocketChannel ch) {
        this.shard = shard;
        this.ch = ch;
    }

    // -----------------------------------------------------------------------
    // I/O events
    // -----------------------------------------------------------------------

    void onReadable() {
        int n;
        try {
            n = rbuf.readFrom(ch, READ_CHUNK);
        } catch (IOException e) {
            close();
            return;
        }
        if (n < 0) {
            close(); // peer closed
            return;
        }
        if (replicaOut != null) {
            onReplicaInput();
        } else {
            pendingInput = true;
            resume();
        }
    }

    void onWritable() {
        flush();
        if (closed || writePending) {
            return;
        }
        if (replicaOut != null) {
            drainReplica();
        } else {
            deliverMessages();
            resume();
        }
    }

    private void updateInterest() {
        if (closed || !key.isValid()) {
            return;
        }
        int ops = 0;
        if (writePending) {
            ops |= SelectionKey.OP_WRITE;
        }
        if (replicaOut != null || (!busy && !writePending && !closeAfterFlush)) {
            ops |= SelectionKey.OP_READ;
        }
        if (key.interestOps() != ops) {
            key.interestOps(ops);
        }
    }

    /** Write as much of {@code wbuf} as the socket takes. */
    private void flush() {
        if (closed) {
            return;
        }
        try {
            while (!wbuf.isEmpty()) {
                if (wbuf.writeTo(ch) == 0) {
                    writePending = true;
                    updateInterest();
                    return;
                }
            }
        } catch (IOException e) {
            close();
            return;
        }
        writePending = false;
        if (closeAfterFlush) {
            close();
            return;
        }
        updateInterest();
    }

    void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (key != null) {
            key.cancel();
        }
        try {
            ch.close();
        } catch (IOException e) {
            // ignore
        }
        shard.connections.remove(this);
        shard.connectedClients = Math.max(0, shard.connectedClients - 1);
        if (ps != null) {
            ps.close();
        }
        if (replicaOut != null) {
            shard.router.repl.remove(replicaId);
        }
    }

    // -----------------------------------------------------------------------
    // Command processing
    // -----------------------------------------------------------------------

    /** Re-entrancy-safe entry point: callbacks that complete synchronously loop here. */
    private void resume() {
        if (inProcess) {
            again = true;
            return;
        }
        inProcess = true;
        try {
            do {
                again = false;
                process();
            } while (again && !closed);
        } finally {
            inProcess = false;
        }
    }

    private void process() {
        if (busy || closed || writePending || replicaOut != null || closeAfterFlush) {
            return;
        }
        pendingInput = false;
        Batch batch = new Batch(shard, Mode.CLIENT);
        boolean close = false;
        // Plan every complete frame first so pipelined requests are answered
        // with a single write syscall.
        while (true) {
            Frame frame;
            try {
                frame = Resp.parse(rbuf);
            } catch (ProtocolException e) {
                batch.reply(Frame.error("ERR Protocol error: " + e.getMessage()));
                close = true;
                break;
            }
            if (frame == null) {
                break;
            }
            Bytes[] args;
            try {
                args = frameToArgs(frame);
            } catch (CmdError e) {
                batch.reply(e.frame);
                close = true;
                break;
            }
            if (args.length == 0) {
                continue; // empty inline line
            }
            shard.commands++;
            Flow flow = route(batch, args);
            if (flow == Flow.CLOSE) {
                close = true;
                break;
            }
            if (flow == Flow.ASYNC) {
                busy = true;
                pendingInput = true; // the rest of rbuf is parsed once the command is done
                updateInterest();
                batch.finish(wbuf, () -> runAsync(args, reply -> {
                    reply.encode(wbuf);
                    busy = false;
                    resume();
                }));
                return;
            }
            if (flow == Flow.PSYNC) {
                busy = true;
                updateInterest();
                batch.finish(wbuf, () -> {
                    busy = false;
                    startReplica(args);
                });
                return;
            }
        }
        if (batch.isEmpty() && !close) {
            afterBatch(false);
            return;
        }
        boolean closeNow = close;
        busy = true;
        batch.finish(wbuf, () -> {
            busy = false;
            afterBatch(closeNow);
        });
    }

    private void afterBatch(boolean close) {
        if (closed) {
            return;
        }
        if (close || (ps != null && ps.overflowed())) {
            closeAfterFlush = true;
        }
        flush();
        if (closed || writePending) {
            return;
        }
        deliverMessages();
        if (!closed && !writePending) {
            updateInterest();
            if (pendingInput) {
                resume();
            }
        }
    }

    /** Subscribed mode: move queued Pub/Sub messages into one write. */
    private void deliverMessages() {
        if (ps == null || closed || busy || writePending || ps.count() == 0) {
            if (ps != null && ps.overflowed() && !busy && !closed) {
                closeAfterFlush = true;
                flush();
            }
            return;
        }
        Bytes m;
        boolean any = false;
        while ((m = ps.poll()) != null) {
            wbuf.put(m);
            any = true;
        }
        if (ps.overflowed()) {
            closeAfterFlush = true;
        }
        if (any || closeAfterFlush) {
            flush();
        }
    }

    private void runAsync(Bytes[] args, Consumer<Frame> cb) {
        switch (Commands.name(args[0])) {
            case "INFO" -> Admin.info(shard, args, cb);
            case "BGREWRITEAOF" -> Admin.bgrewriteaof(shard, cb);
            case "WAIT" -> {
                if (args.length != 3) {
                    cb.accept(Commands.wrongArgs(args));
                    return;
                }
                long n;
                long t;
                try {
                    n = Commands.parseInt(args[1]);
                    t = Commands.parseInt(args[2]);
                } catch (CmdError e) {
                    cb.accept(e.frame);
                    return;
                }
                if (n < 0 || t < 0) {
                    cb.accept(Frame.error("ERR timeout is negative"));
                    return;
                }
                shard.router.repl.waitFor((int) Math.min(n, Integer.MAX_VALUE), t, shard, cb);
            }
            default -> cb.accept(Commands.unknownCommand(args));
        }
    }

    private Flow route(Batch batch, Bytes[] args) {
        String name = Commands.name(args[0]);
        boolean subscribed = ps != null && ps.count() > 0;
        Router router = shard.router;
        List<Bytes> rest = Arrays.asList(args).subList(1, args.length);

        switch (name) {
            case "QUIT":
                batch.reply(Frame.OK);
                return Flow.CLOSE;
            case "SUBSCRIBE":
            case "PSUBSCRIBE": {
                if (args.length < 2) {
                    batch.reply(Commands.wrongArgs(args));
                    return Flow.CONTINUE;
                }
                if (ps == null) {
                    ps = new PubSub.Client(router.pubsub, router.nextClientId(), shard, this::deliverMessages);
                }
                ByteBuf out = new ByteBuf(64);
                if (name.equals("SUBSCRIBE")) {
                    ps.subscribe(rest, out);
                } else {
                    ps.psubscribe(rest, out);
                }
                batch.raw(out.toBytes());
                return Flow.CONTINUE;
            }
            case "UNSUBSCRIBE":
            case "PUNSUBSCRIBE": {
                ByteBuf out = new ByteBuf(64);
                String kind = name.equals("UNSUBSCRIBE") ? "unsubscribe" : "punsubscribe";
                if (ps == null) {
                    // Not subscribed to anything: Redis still answers once per request.
                    Frame.array(Frame.bulk(kind), Frame.NULL, Frame.integer(0)).encode(out);
                } else if (name.equals("UNSUBSCRIBE")) {
                    ps.unsubscribe(rest, out);
                } else {
                    ps.punsubscribe(rest, out);
                }
                batch.raw(out.toBytes());
                return Flow.CONTINUE;
            }
            case "PUBLISH":
                if (args.length != 3) {
                    batch.reply(Commands.wrongArgs(args));
                } else {
                    batch.reply(Frame.integer(router.pubsub.publish(args[1], args[2])));
                }
                return Flow.CONTINUE;
            case "PUBSUB":
                batch.reply(pubsubIntrospection(router.pubsub, args));
                return Flow.CONTINUE;
            default:
                break;
        }
        if (subscribed) {
            if (name.equals("PING")) {
                // RESP2 subscribed-mode PING replies with a ["pong", payload] array.
                Bytes payload = args.length > 1 ? args[1] : Bytes.EMPTY;
                batch.reply(Frame.array(Frame.bulk("pong"), new Frame.Bulk(payload)));
            } else {
                batch.reply(Frame.error("ERR Can't execute '" + Commands.cmdName(args)
                        + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context"));
            }
            return Flow.CONTINUE;
        }
        switch (name) {
            case "INFO", "BGREWRITEAOF", "WAIT" -> {
                return Flow.ASYNC;
            }
            case "PSYNC", "SYNC" -> {
                return Flow.PSYNC;
            }
            case "REPLCONF" -> {
                if (args.length >= 3 && args[1].equalsIgnoreCase("listening-port")) {
                    try {
                        int p = Integer.parseInt(args[2].toString());
                        listeningPort = (p >= 0 && p <= 65535) ? p : -1;
                    } catch (NumberFormatException e) {
                        listeningPort = -1;
                    }
                }
                batch.reply(Frame.OK);
                return Flow.CONTINUE;
            }
            case "REPLICAOF", "SLAVEOF" -> {
                batch.reply(replicaof(args));
                return Flow.CONTINUE;
            }
            case "CONFIG" -> {
                if (args.length >= 3 && args[1].equalsIgnoreCase("GET")) {
                    batch.reply(configGet(router.aofEnabled, Arrays.asList(args).subList(2, args.length)));
                    return Flow.CONTINUE;
                }
            }
            case "CLIENT" -> {
                if (args.length == 4 && args[1].equalsIgnoreCase("KILL") && args[2].equalsIgnoreCase("TYPE")
                        && (args[3].equalsIgnoreCase("replica") || args[3].equalsIgnoreCase("slave"))) {
                    batch.reply(Frame.integer(router.repl.killReplicas()));
                    return Flow.CONTINUE;
                }
            }
            default -> {}
        }
        batch.command(args);
        return Flow.CONTINUE;
    }

    /**
     * {@code CONFIG GET pattern [pattern ...]} over the few settings clients
     * probe ({@code redis-benchmark} asks for {@code save} and
     * {@code appendonly} at startup).
     */
    private static Frame configGet(boolean aof, List<Bytes> patterns) {
        String[][] params = {
            {"save", ""}, {"appendonly", aof ? "yes" : "no"}, {"databases", "1"}, {"maxmemory", "0"},
        };
        List<Frame> out = new ArrayList<>();
        for (String[] kv : params) {
            for (Bytes p : patterns) {
                if (Glob.match(p.toLowerAscii().array(), kv[0].getBytes(java.nio.charset.StandardCharsets.US_ASCII))) {
                    out.add(Frame.bulk(kv[0]));
                    out.add(Frame.bulk(kv[1]));
                    break;
                }
            }
        }
        return Frame.array(out);
    }

    /** {@code REPLICAOF host port | NO ONE} */
    private Frame replicaof(Bytes[] args) {
        if (args.length != 3) {
            return Commands.wrongArgs(args);
        }
        if (args[1].equalsIgnoreCase("NO") && args[2].equalsIgnoreCase("ONE")) {
            Replication.replicaof(shard, null, 0);
            return Frame.OK;
        }
        int port;
        try {
            port = Integer.parseInt(args[2].toString());
        } catch (NumberFormatException e) {
            port = -1;
        }
        if (port < 0 || port > 65535) {
            return Frame.error("ERR Invalid master port");
        }
        Replication.replicaof(shard, args[1].toString(), port);
        return Frame.OK;
    }

    /** {@code PUBSUB CHANNELS [pattern] | NUMSUB [channel ...] | NUMPAT} */
    private static Frame pubsubIntrospection(PubSub reg, Bytes[] args) {
        String sub = args.length > 1 ? args[1].toUpperAscii() : "";
        switch (sub) {
            case "CHANNELS":
                if (args.length <= 3) {
                    List<Frame> out = new ArrayList<>();
                    for (Bytes c : reg.activeChannels(args.length == 3 ? args[2] : null)) {
                        out.add(new Frame.Bulk(c));
                    }
                    return Frame.array(out);
                }
                break;
            case "NUMSUB": {
                List<Frame> out = new ArrayList<>();
                for (int i = 2; i < args.length; i++) {
                    out.add(new Frame.Bulk(args[i]));
                    out.add(Frame.integer(reg.numSubscribers(args[i])));
                }
                return Frame.array(out);
            }
            case "NUMPAT":
                if (args.length == 2) {
                    return Frame.integer(reg.numPatterns());
                }
                break;
            default:
                break;
        }
        return Commands.wrongArgs(args);
    }

    /** Requests must be arrays of bulk strings. */
    private static Bytes[] frameToArgs(Frame frame) throws CmdError {
        if (!(frame instanceof Frame.Array arr)) {
            throw new CmdError(Frame.error("ERR Protocol error: expected array of bulk strings"));
        }
        Bytes[] args = new Bytes[arr.items().size()];
        for (int i = 0; i < args.length; i++) {
            args[i] = switch (arr.items().get(i)) {
                case Frame.Bulk b -> b.value();
                case Frame.Simple s -> Bytes.of(s.value());
                case Frame.Int n -> Bytes.ofLong(n.value());
                default -> throw new CmdError(Frame.error("ERR Protocol error: expected bulk string"));
            };
        }
        return args;
    }

    // -----------------------------------------------------------------------
    // Leader side of replication: this connection becomes a replica stream
    // -----------------------------------------------------------------------

    /** Take over a connection that sent {@code PSYNC}/{@code SYNC} and stream to it. */
    private void startReplica(Bytes[] args) {
        if (closed) {
            return;
        }
        flush();
        Replication repl = shard.router.repl;
        String replid = "?";
        long offset = -1;
        if (args.length >= 3) {
            replid = args[1].toString();
            try {
                offset = Long.parseLong(args[2].toString());
            } catch (NumberFormatException e) {
                offset = -1;
            }
        }
        String ip = "?";
        int peerPort = 0;
        try {
            if (ch.getRemoteAddress() instanceof InetSocketAddress a) {
                ip = a.getAddress().getHostAddress();
                peerPort = a.getPort();
            }
        } catch (IOException e) {
            close();
            return;
        }
        int port = listeningPort >= 0 ? listeningPort : peerPort;
        replicaOut = new Outbox(Replication.REPLICA_QUEUE, shard, this::drainReplica);
        replicaId = repl.nextReplicaId();
        if (ps != null) {
            ps.close();
            ps = null;
        }
        switch (repl.register(replicaId, replicaOut, ip, port, replid, offset)) {
            case CONTINUE -> Log.info("replica " + ip + ":" + port + " resumed at offset " + offset);
            case FULL -> {
                Log.info("replica " + ip + ":" + port + " needs a full sync");
                long id = replicaId;
                shard.router.broadcast(() -> new ShardMsg.Run(sh -> sh.replSnapshot(id)));
            }
        }
        updateInterest();
        if (!rbuf.isEmpty()) {
            onReplicaInput();
        }
    }

    /** Move queued stream chunks to the socket; close once the hub dropped us. */
    private void drainReplica() {
        if (closed || replicaOut == null) {
            return;
        }
        Bytes chunk;
        while ((chunk = replicaOut.poll()) != null) {
            wbuf.put(chunk);
        }
        if (replicaOut.isClosed() && replicaOut.isEmpty()) {
            closeAfterFlush = true; // dropped by the hub
        }
        if (!writePending) {
            flush();
        }
    }

    /** The replica only ever sends {@code REPLCONF ACK <offset>}. */
    private void onReplicaInput() {
        while (true) {
            Frame f;
            try {
                f = Resp.parse(rbuf);
            } catch (ProtocolException e) {
                close();
                return;
            }
            if (f == null) {
                return;
            }
            if (f instanceof Frame.Array a && a.items().size() == 3
                    && a.items().get(0) instanceof Frame.Bulk c && c.value().equalsIgnoreCase("REPLCONF")
                    && a.items().get(1) instanceof Frame.Bulk sub && sub.value().equalsIgnoreCase("ACK")
                    && a.items().get(2) instanceof Frame.Bulk off) {
                try {
                    shard.router.repl.ack(replicaId, Long.parseLong(off.value().toString()));
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }
    }
}
