package forgelog;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * The broker: topics split into partitions, each a {@link PartitionLog}; a record's partition is its key's hash,
 * so records with the same key stay in order. A virtual thread serves each connection. Topics are created on
 * first use with {@code defaultPartitions} partitions.
 *
 * <pre>java -jar forgelog.jar --port 9192 --data ./data --partitions 3 --fsync batch</pre>
 * {@code --fsync batch} (default) fsyncs once per produce request before acknowledging; {@code none} leaves it to the OS.
 */
public final class Broker implements Closeable {

    private final Path dir;
    private final int defaultPartitions;
    private final boolean fsync;
    private final long sessionMs;
    private final Map<String, PartitionLog[]> topics = new ConcurrentHashMap<>();
    private final Map<String, Coordinator> coordinators = new ConcurrentHashMap<>();
    private final Offsets offsets;
    private final AtomicInteger roundRobin = new AtomicInteger();
    private final ServerSocket server;
    private volatile boolean closed;

    public Broker(Path dir, int port, int defaultPartitions, boolean fsync, long sessionMs) throws IOException {
        this.dir = dir;
        this.defaultPartitions = defaultPartitions;
        this.fsync = fsync;
        this.sessionMs = sessionMs;
        this.offsets = new Offsets(dir);
        try (Stream<Path> existing = Files.isDirectory(dir) ? Files.list(dir) : Stream.<Path>empty()) {
            for (Path topicDir : existing.filter(Files::isDirectory).toList()) {
                log(topicDir.getFileName().toString());      // recover every topic now, not on first use
            }
        }
        this.server = new ServerSocket(port, 256, InetAddress.getByName("0.0.0.0"));
        Thread.ofPlatform().name("forgelog-accept").daemon(true).start(this::acceptLoop);
    }

    public int port() {
        return server.getLocalPort();
    }

    public static void main(String[] args) throws Exception {
        int port = 9192;
        int partitions = 3;
        boolean fsync = true;
        Path data = Path.of("forgelog-data");
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> port = Integer.parseInt(args[++i]);
                case "--data" -> data = Path.of(args[++i]);
                case "--partitions" -> partitions = Integer.parseInt(args[++i]);
                case "--fsync" -> fsync = !args[++i].equals("none");
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        Broker b = new Broker(data, port, partitions, fsync, 10_000);
        System.out.println("forgelog listening on :" + b.port() + " data=" + data + " partitions=" + partitions + " fsync=" + fsync);
        Thread.currentThread().join();
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket s = server.accept();
                s.setTcpNoDelay(true);
                Thread.ofVirtual().name("forgelog-conn").start(() -> serve(s));
            } catch (IOException e) {
                if (!closed) {
                    System.err.println("accept failed: " + e);
                }
            }
        }
    }

    private void serve(Socket s) {
        try (s; var in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 65536));
             var out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream(), 65536))) {
            while (true) {
                DataInputStream req = Wire.readFrame(in);
                Wire.Out resp = new Wire.Out();
                try {
                    resp.data.writeByte(0);
                    handle(req, resp.data);
                } catch (IOException | RuntimeException e) {
                    resp = new Wire.Out();
                    resp.data.writeByte(1);
                    Wire.writeString(resp.data, e.toString());
                }
                resp.send(out);
            }
        } catch (EOFException e) {
            // client closed the connection
        } catch (IOException | InterruptedException e) {
            if (!closed) {
                System.err.println("connection ended: " + e);
            }
        }
    }

    private void handle(DataInputStream in, DataOutputStream out) throws IOException, InterruptedException {
        byte op = in.readByte();
        switch (op) {
            case Wire.META -> out.writeInt(log(Wire.readString(in)).length);
            case Wire.PRODUCE -> produce(in, out);
            case Wire.FETCH -> fetch(in, out);
            case Wire.COMMIT -> {
                String group = Wire.readString(in), topic = Wire.readString(in);
                offsets.commit(group, topic, in.readInt(), in.readLong());
            }
            case Wire.COMMITTED -> {
                String group = Wire.readString(in), topic = Wire.readString(in);
                out.writeLong(offsets.get(group, topic, in.readInt()));
            }
            case Wire.JOIN -> {
                String group = Wire.readString(in), topic = Wire.readString(in), member = Wire.readString(in);
                Coordinator c = coordinator(group, topic);
                out.writeInt(c.join(member, System.currentTimeMillis()));
                List<Integer> mine = c.assigned(member);
                out.writeInt(mine.size());
                for (int p : mine) {
                    out.writeInt(p);
                }
            }
            case Wire.HEARTBEAT -> {
                String group = Wire.readString(in), topic = Wire.readString(in), member = Wire.readString(in);
                out.writeInt(coordinator(group, topic).heartbeat(member, System.currentTimeMillis()));
            }
            case Wire.LEAVE -> {
                String group = Wire.readString(in), topic = Wire.readString(in);
                coordinator(group, topic).leave(Wire.readString(in));
            }
            default -> throw new IOException("unknown opcode " + op);
        }
    }

    private void produce(DataInputStream in, DataOutputStream out) throws IOException {
        String topic = Wire.readString(in);
        PartitionLog[] parts = log(topic);
        int n = in.readInt();
        long now = System.currentTimeMillis();
        List<List<Rec>> perPartition = new ArrayList<>();
        for (int p = 0; p < parts.length; p++) {
            perPartition.add(new ArrayList<>());
        }
        int[] partitionOf = new int[n];
        int[] indexIn = new int[n];
        for (int i = 0; i < n; i++) {
            byte[] key = Wire.readBytes(in);
            String type = Wire.readString(in), id = Wire.readString(in);
            byte[] value = Wire.readBytes(in);
            int p = key == null ? Math.floorMod(roundRobin.getAndIncrement(), parts.length) : Math.floorMod(Arrays.hashCode(key), parts.length);
            partitionOf[i] = p;
            indexIn[i] = perPartition.get(p).size();
            perPartition.get(p).add(new Rec(0, now, key, type, id, value));
        }
        long[] first = new long[parts.length];
        for (int p = 0; p < parts.length; p++) {
            if (!perPartition.get(p).isEmpty()) {
                first[p] = parts[p].append(perPartition.get(p), fsync);
            }
        }
        out.writeInt(n);
        for (int i = 0; i < n; i++) {
            out.writeInt(partitionOf[i]);
            out.writeLong(first[partitionOf[i]] + indexIn[i]);
        }
    }

    private void fetch(DataInputStream in, DataOutputStream out) throws IOException, InterruptedException {
        PartitionLog[] parts = log(Wire.readString(in));
        int partition = in.readInt();
        long offset = in.readLong();
        int max = in.readInt();
        long waitMs = in.readLong();
        List<Rec> recs = parts[partition].read(offset, max, waitMs);
        out.writeInt(recs.size());
        for (Rec r : recs) {
            out.writeLong(r.offset());
            out.writeLong(r.timestamp());
            Wire.writeBytes(out, r.key());
            Wire.writeString(out, r.type());
            Wire.writeString(out, r.eventId());
            Wire.writeBytes(out, r.value());
        }
    }

    private Coordinator coordinator(String group, String topic) throws IOException {
        int partitions = log(topic).length;
        return coordinators.computeIfAbsent(group + "\t" + topic, k -> new Coordinator(partitions, sessionMs));
    }

    private PartitionLog[] log(String topic) throws IOException {
        PartitionLog[] existing = topics.get(topic);
        if (existing != null) {
            return existing;
        }
        synchronized (topics) {
            existing = topics.get(topic);
            if (existing != null) {
                return existing;
            }
            Path td = dir.resolve(topic);
            int n = defaultPartitions;
            if (Files.isDirectory(td)) {
                try (Stream<Path> s = Files.list(td)) {
                    n = (int) s.filter(Files::isDirectory).count();
                }
            }
            PartitionLog[] made = new PartitionLog[n];
            for (int p = 0; p < n; p++) {
                made[p] = new PartitionLog(td.resolve(Integer.toString(p)));
            }
            topics.put(topic, made);
            return made;
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        server.close();
        for (PartitionLog[] parts : topics.values()) {
            for (PartitionLog l : parts) {
                l.close();
            }
        }
    }
}
