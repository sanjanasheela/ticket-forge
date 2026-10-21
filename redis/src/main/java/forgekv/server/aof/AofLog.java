package forgekv.server.aof;

import forgekv.resp.Frame;
import forgekv.resp.ProtocolException;
import forgekv.resp.Resp;
import forgekv.server.KeySlot;
import forgekv.server.cmd.Commands;
import forgekv.storage.Clock;
import forgekv.storage.Db;
import forgekv.storage.Entry;
import forgekv.storage.Value;
import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import forgekv.util.Log;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Append-only-file persistence: canonical logging, replay and layout.
 *
 * <p>Every state-changing command is appended to the AOF in RESP form and
 * replayed on startup. Four design points make replay exact:
 *
 * <ol>
 *   <li><b>Commands are logged in a canonical, time-independent form.</b>
 *       {@code SET k v EX 10} is logged as {@code SET k v PXAT <abs>},
 *       {@code EXPIRE} becomes {@code PEXPIREAT}, and conditional commands
 *       ({@code SET NX}, {@code CAS}, {@code DECRIF}...) are only logged when
 *       they actually changed something.
 *   <li><b>Expiry is logged as {@code DEL}.</b> Replay runs on a <em>frozen
 *       clock</em> ({@code now = 0}) so a TTL can never fire mid-replay and
 *       change what a later {@code INCR} sees. The {@code DEL} records emitted
 *       when keys expired reproduce those events. Expiry {@code DEL}s are
 *       written <em>before</em> the command that triggered them.
 *   <li><b>A torn tail is tolerated.</b> A crash mid-write leaves a partial
 *       final command; it is detected, reported and truncated away on load.
 *   <li><b>One log per shard, each self-contained.</b> Every shard appends to
 *       its own file, and {@code FLUSHALL} is logged as
 *       {@code FLUSHSHARD <shard> <n>}, which only clears keys that shard
 *       owns. The logs are therefore independent: they replay correctly in
 *       any order, into any number of shards.
 * </ol>
 *
 * <p>The same canonical byte stream feeds replication (see
 * {@link forgekv.server.repl.Replication}).
 *
 * <h2>On-disk layout</h2>
 *
 * <p>{@code --appendfilename appendonly.aof} names a <em>family</em> of
 * files: {@code appendonly.manifest} records the current generation and shard
 * count, and shard {@code i} appends to {@code appendonly.g<gen>.s<i>.aof}.
 * Starting with a different {@code --shards} re-partitions the data into a
 * new generation, then atomically swaps the manifest. A crash part-way
 * through leaves the old generation in charge. A pre-sharding single
 * {@code appendonly.aof} is imported the same way.
 */
public final class AofLog {
    /** Elements per command when snapshotting aggregates during a rewrite. */
    static final int REWRITE_CHUNK = 256;
    static final String MANIFEST_MAGIC = "forgekv-aof 1";

    private AofLog() {}

    // -----------------------------------------------------------------------
    // Encoding & canonical logging
    // -----------------------------------------------------------------------

    /** Serialize a command as a RESP array of bulk strings. */
    public static void encodeCommand(ByteBuf out, Bytes... parts) {
        out.put((byte) '*').putLong(parts.length).crlf();
        for (Bytes p : parts) {
            out.ensureWritable(p.length() + 16);
            out.put((byte) '$').putLong(p.length()).crlf().put(p).crlf();
        }
    }

    public static void encodeCommand(ByteBuf out, List<Bytes> parts) {
        encodeCommand(out, parts.toArray(Bytes[]::new));
    }

    private static final Bytes SET = Bytes.of("SET");
    private static final Bytes PXAT = Bytes.of("PXAT");
    private static final Bytes RPUSH = Bytes.of("RPUSH");
    private static final Bytes HSET = Bytes.of("HSET");
    private static final Bytes SADD = Bytes.of("SADD");
    private static final Bytes PEXPIREAT = Bytes.of("PEXPIREAT");
    public static final Bytes DEL = Bytes.of("DEL");
    private static final Bytes FLUSHSHARD = Bytes.of("FLUSHSHARD");

    private static Bytes num(long n) {
        return Bytes.ofLong(n);
    }

    /** Emit the commands that recreate a single key (value + TTL). */
    public static void emitEntry(Bytes key, Entry entry, Consumer<Bytes[]> emit) {
        long ttl = entry.expiresAt();
        boolean hasTtl = entry.hasExpiry();
        switch (entry.value()) {
            case Value.Str s -> {
                if (hasTtl) {
                    emit.accept(new Bytes[] {SET, key, s.value(), PXAT, num(ttl)});
                } else {
                    emit.accept(new Bytes[] {SET, key, s.value()});
                }
            }
            case Value.ListV l -> {
                List<Bytes> parts = new ArrayList<>();
                for (Bytes item : l.items()) {
                    if (parts.isEmpty()) {
                        parts.add(RPUSH);
                        parts.add(key);
                    }
                    parts.add(item);
                    if (parts.size() - 2 == REWRITE_CHUNK) {
                        emit.accept(parts.toArray(Bytes[]::new));
                        parts.clear();
                    }
                }
                if (!parts.isEmpty()) {
                    emit.accept(parts.toArray(Bytes[]::new));
                }
            }
            case Value.HashV h -> {
                List<Bytes> parts = new ArrayList<>();
                for (Map.Entry<Bytes, Bytes> e : h.map().entrySet()) {
                    if (parts.isEmpty()) {
                        parts.add(HSET);
                        parts.add(key);
                    }
                    parts.add(e.getKey());
                    parts.add(e.getValue());
                    if ((parts.size() - 2) / 2 == REWRITE_CHUNK) {
                        emit.accept(parts.toArray(Bytes[]::new));
                        parts.clear();
                    }
                }
                if (!parts.isEmpty()) {
                    emit.accept(parts.toArray(Bytes[]::new));
                }
            }
            case Value.SetV s -> {
                List<Bytes> parts = new ArrayList<>();
                for (Bytes m : s.set()) {
                    if (parts.isEmpty()) {
                        parts.add(SADD);
                        parts.add(key);
                    }
                    parts.add(m);
                    if (parts.size() - 2 == REWRITE_CHUNK) {
                        emit.accept(parts.toArray(Bytes[]::new));
                        parts.clear();
                    }
                }
                if (!parts.isEmpty()) {
                    emit.accept(parts.toArray(Bytes[]::new));
                }
            }
        }
        // Strings carry their TTL inline (PXAT); aggregates need a follow-up command.
        if (!(entry.value() instanceof Value.Str) && hasTtl) {
            emit.accept(new Bytes[] {PEXPIREAT, key, num(ttl)});
        }
    }

    /** Encode every live key of {@code db} as commands (AOF rewrite, replication sync). */
    public static void snapshotInto(ByteBuf out, Db db, long now) {
        db.forEachLive(now, (key, entry) -> emitEntry(key, entry, parts -> encodeCommand(out, parts)));
    }

    /**
     * Append the canonical effect of an executed command to {@code out}.
     *
     * <p>{@code shardIdx}/{@code shardCount} identify the shard that ran it.
     * Called after the handler ran, so a key's post-command state can be read
     * back. Must run for <em>every</em> command (reads included): a read may
     * have lazily expired a key, and that expiry must be logged before
     * anything that follows.
     */
    public static void feed(ByteBuf out, Db db, int shardIdx, int shardCount, Bytes[] args, Frame reply) {
        for (Bytes key : db.drainExpired()) {
            encodeCommand(out, DEL, key);
        }
        if (reply instanceof Frame.Error) {
            return;
        }
        String name = Commands.name(args[0]);
        boolean int1 = reply instanceof Frame.Int i && i.value() == 1;
        boolean nonzero = reply instanceof Frame.Int i && i.value() != 0;
        boolean notNull = !(reply instanceof Frame.Null || reply instanceof Frame.NullArray);

        switch (name) {
            // Log the key's resulting state: exact value and absolute deadline.
            case "SET", "SETNX" -> {
                boolean hasGet = false;
                for (int i = Math.min(3, args.length); i < args.length; i++) {
                    hasGet |= args[i].equalsIgnoreCase("GET");
                }
                boolean changed = name.equals("SETNX") ? int1 : (hasGet || notNull);
                if (changed) {
                    Entry e = db.peek(args[1]);
                    if (e != null) {
                        emitEntry(args[1], e, parts -> encodeCommand(out, parts));
                    }
                }
            }
            case "GETDEL" -> {
                if (notNull) {
                    encodeCommand(out, DEL, args[1]);
                }
            }
            case "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT" -> {
                if (int1) {
                    Entry e = db.peek(args[1]);
                    if (e != null && e.hasExpiry()) {
                        encodeCommand(out, PEXPIREAT, args[1], num(e.expiresAt()));
                    } else {
                        // A deadline in the past deleted the key.
                        encodeCommand(out, DEL, args[1]);
                    }
                }
            }
            // Scope flushes to this shard's keys (see class docs, point 4).
            case "FLUSHALL", "FLUSHDB", "FLUSHSHARD" -> {
                List<Bytes> parts = new ArrayList<>();
                parts.add(FLUSHSHARD);
                parts.add(num(shardIdx));
                parts.add(num(shardCount));
                if (name.equals("FLUSHSHARD")) {
                    for (int i = 1; i < args.length; i++) {
                        parts.add(args[i]);
                    }
                }
                encodeCommand(out, parts);
            }
            // Deterministic given prior state: log as-is when they changed something.
            case "DEL", "UNLINK", "HDEL", "SREM" -> {
                if (nonzero) {
                    encodeCommand(out, args);
                }
            }
            case "PERSIST", "CAS", "CAD" -> {
                if (int1) {
                    encodeCommand(out, args);
                }
            }
            case "LPOP", "RPOP", "DECRIF" -> {
                if (notNull) {
                    encodeCommand(out, args);
                }
            }
            case "MSET", "APPEND", "INCR", "DECR", "INCRBY", "DECRBY", "LPUSH", "RPUSH", "HSET", "HMSET", "HINCRBY",
                    "SADD" -> encodeCommand(out, args);
            default -> {}
        }
    }

    // -----------------------------------------------------------------------
    // Loading & layout
    // -----------------------------------------------------------------------

    /**
     * Result of replaying an AOF.
     *
     * @param truncatedBytes bytes of a torn final command that were discarded
     */
    public record LoadReport(long commands, long errors, long truncatedBytes) {}

    /** The AOF is damaged somewhere other than its tail. */
    public static final class CorruptedException extends IOException {
        CorruptedException(String msg) {
            super(msg);
        }
    }

    /**
     * Replay {@code path} into {@code db} on a frozen clock ({@code now = 0},
     * see class docs).
     *
     * <p>A partial trailing command (crash mid-write) is truncated away.
     * Corruption anywhere else is an error: starting on a half-loaded dataset
     * would be worse than refusing to start.
     */
    public static LoadReport load(Path path, Db db) throws IOException {
        long commands = 0;
        long errors = 0;
        long goodOffset = 0;
        ByteBuf buf = new ByteBuf(1 << 20);
        try (InputStream in = Files.newInputStream(path)) {
            while (true) {
                while (true) {
                    int before = buf.readable();
                    Frame frame;
                    try {
                        frame = Resp.parse(buf);
                    } catch (ProtocolException e) {
                        throw corrupt(path, goodOffset, e.getMessage());
                    }
                    if (frame == null) {
                        break;
                    }
                    goodOffset += before - buf.readable();
                    if (!(frame instanceof Frame.Array arr)) {
                        throw corrupt(path, goodOffset, "record is not a command array");
                    }
                    Bytes[] args = new Bytes[arr.items().size()];
                    boolean ok = !arr.items().isEmpty();
                    for (int i = 0; ok && i < args.length; i++) {
                        if (arr.items().get(i) instanceof Frame.Bulk b) {
                            args[i] = b.value();
                        } else {
                            ok = false;
                        }
                    }
                    if (!ok) {
                        throw corrupt(path, goodOffset, "malformed command record");
                    }
                    commands++;
                    if (Commands.execute(db, 0, args) instanceof Frame.Error e) {
                        errors++;
                        if (errors <= 5) {
                            Log.warn("AOF replay: command #" + commands + " failed: " + e.message());
                        }
                    }
                }
                int n = buf.readFrom(in, 1 << 16);
                if (n < 0) {
                    break;
                }
            }
        }
        long truncated = 0;
        if (!buf.isEmpty()) {
            truncated = buf.readable();
            Log.warn(path + " ends with an incomplete command (" + truncated
                    + " bytes, likely a crash mid-write); truncating to " + goodOffset + " bytes");
            try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
                ch.truncate(goodOffset);
            }
        }
        Log.info("AOF " + path + " loaded: " + commands + " commands replayed (" + errors + " errors, " + truncated
                + " bytes truncated)");
        return new LoadReport(commands, errors, truncated);
    }

    private static CorruptedException corrupt(Path path, long offset, String why) {
        return new CorruptedException("AOF " + path + " is corrupted near byte " + offset + ": " + why);
    }

    /** Generation + shard count recorded in the manifest. */
    public record Layout(long generation, int shards) {}

    /** {@code dir/appendonly.aof} + {@code suffix} → {@code dir/appendonly.<suffix>}. */
    private static Path sibling(Path base, String suffix) {
        Path name = base.getFileName();
        String stem = "appendonly";
        if (name != null) {
            String s = name.toString();
            int dot = s.lastIndexOf('.');
            stem = dot > 0 ? s.substring(0, dot) : s;
        }
        return base.resolveSibling(stem + "." + suffix);
    }

    public static Path manifestPath(Path base) {
        return sibling(base, "manifest");
    }

    public static Path shardPath(Path base, long generation, int shard) {
        return sibling(base, "g" + generation + ".s" + shard + ".aof");
    }

    /** The current layout, or {@code null} if there is no manifest. */
    public static Layout readManifest(Path base) throws IOException {
        String text;
        try {
            text = Files.readString(manifestPath(base));
        } catch (NoSuchFileException e) {
            return null;
        }
        IOException bad = new IOException("malformed AOF manifest: \"" + text.replace("\n", "\\n") + "\"");
        List<String> lines = text.lines().toList();
        if (lines.isEmpty() || !lines.get(0).equals(MANIFEST_MAGIC)) {
            throw bad;
        }
        Long generation = null;
        Integer shards = null;
        for (String line : lines.subList(1, lines.size())) {
            int sp = line.indexOf(' ');
            if (sp < 0) {
                continue;
            }
            String k = line.substring(0, sp);
            String v = line.substring(sp + 1);
            try {
                if (k.equals("generation")) {
                    generation = Long.parseLong(v);
                } else if (k.equals("shards")) {
                    int n = Integer.parseInt(v);
                    shards = n > 0 ? n : null;
                }
            } catch (NumberFormatException e) {
                if (k.equals("generation")) {
                    generation = null;
                } else {
                    shards = null;
                }
            }
        }
        if (generation == null || shards == null) {
            throw bad;
        }
        return new Layout(generation, shards);
    }

    @FunctionalInterface
    interface IOConsumer<T> {
        void accept(T t) throws IOException;
    }

    /** Write {@code path} atomically (temp file + fsync + rename + dir fsync). */
    static void atomicWrite(Path path, IOConsumer<OutputStream> write) throws IOException {
        Path tmp = Path.of(path + ".tmp");
        try (FileOutputStream fos = new FileOutputStream(tmp.toFile());
                BufferedOutputStream w = new BufferedOutputStream(fos, 64 * 1024)) {
            write.accept(w);
            w.flush();
            fos.getFD().sync();
        } catch (IOException | RuntimeException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        Path dir = path.toAbsolutePath().getParent();
        if (dir != null) {
            // Persist the rename itself; best effort (not supported everywhere).
            try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
                d.force(true);
            } catch (IOException e) {
                // ignore
            }
        }
    }

    static void writeManifest(Path base, Layout layout) throws IOException {
        atomicWrite(manifestPath(base), w -> w.write((MANIFEST_MAGIC + "\ngeneration " + layout.generation()
                + "\nshards " + layout.shards() + "\n").getBytes(java.nio.charset.StandardCharsets.US_ASCII)));
    }

    /** Atomically replace {@code path} with the commands that recreate {@code db}. */
    static void writeSnapshot(Path path, Db db, long now) throws IOException {
        atomicWrite(path, w -> {
            ByteBuf scratch = new ByteBuf(4096);
            try {
                db.forEachLive(now, (key, entry) -> {
                    scratch.clear();
                    emitEntry(key, entry, parts -> encodeCommand(scratch, parts));
                    try {
                        w.write(scratch.array(), scratch.readIndex(), scratch.readable());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        });
    }

    /** Split one keyspace into {@code n} shard keyspaces. */
    public static List<Db> partition(Db db, int n) {
        if (n == 1) {
            return new ArrayList<>(List.of(db));
        }
        List<Db> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(new Db());
        }
        for (Map.Entry<Bytes, Entry> e : db.drain()) {
            out.get(KeySlot.shardOf(e.getKey(), n)).insertEntry(e.getKey(), e.getValue());
        }
        return out;
    }

    /** The recovered dataset, already split per shard, plus each shard's AOF file. */
    public record Recovered(List<Db> dbs, List<Path> files, Layout layout) {}

    /**
     * Recover the dataset for {@code n} shards from the AOF family at {@code base}.
     *
     * <p>If the files on disk were written with a different shard count (or by
     * a pre-sharding ForgeKV), the data is re-partitioned into a new
     * generation first. The old files are only deleted after the new manifest
     * is in place.
     */
    public static Recovered recover(Path base, int n) throws IOException {
        Layout manifest = readManifest(base);
        List<Path> sources = new ArrayList<>();
        if (manifest != null) {
            for (int i = 0; i < manifest.shards(); i++) {
                sources.add(shardPath(base, manifest.generation(), i));
            }
        } else if (Files.exists(base)) {
            sources.add(base);
        }

        // Per-shard logs are independent (see class docs), so replaying them
        // all into one keyspace and splitting afterwards is exact.
        Db combined = new Db();
        for (Path src : sources) {
            try {
                load(src, combined);
            } catch (NoSuchFileException e) {
                // A shard that never wrote anything may have no file yet.
                if (manifest == null) {
                    throw e;
                }
            }
        }
        List<Db> dbs = partition(combined, n);

        if (manifest != null && manifest.shards() == n) {
            return new Recovered(dbs, sources, manifest);
        }

        Layout layout = new Layout(manifest == null ? 1 : manifest.generation() + 1, n);
        List<Path> files = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            files.add(shardPath(base, layout.generation(), i));
        }
        long now = Clock.nowMs();
        for (int i = 0; i < n; i++) {
            writeSnapshot(files.get(i), dbs.get(i), now);
        }
        writeManifest(base, layout);
        for (Path src : sources) {
            try {
                Files.deleteIfExists(src);
            } catch (IOException e) {
                // ignore
            }
        }
        if (!sources.isEmpty()) {
            Log.info("AOF re-partitioned from " + sources.size() + " file(s) into " + n + " shard(s) (generation "
                    + layout.generation() + ")");
        }
        return new Recovered(dbs, files, layout);
    }
}
