package forgekv.server;

import forgekv.resp.Frame;
import forgekv.server.cmd.Commands;
import forgekv.server.cmd.Commands.Spec;
import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * The commands of one pipelined read, planned across shards.
 *
 * <ul>
 *   <li>commands whose keys live on the connection's own shard run
 *       <b>inline</b>, immediately, with no synchronisation at all;
 *   <li>commands for other shards are queued per shard and sent as <b>one
 *       message per shard per batch</b>, so a 100-command pipeline spread over
 *       8 shards costs at most 7 cross-thread hops, not 100;
 *   <li>multi-key commands are split per shard and their partial replies
 *       merged ({@code MGET} re-ordered, {@code DEL}/{@code EXISTS} summed,
 *       {@code KEYS} concatenated...).
 * </ul>
 *
 * <p>Ordering: a shard's mailbox is FIFO and the batch sends each shard its
 * sub-commands in pipeline order, so every key observes this connection's
 * commands in the order they were sent. Replies are written back in request
 * order regardless of which shard answers first.
 *
 * <p>Atomicity: every command is atomic <em>per shard</em>. A multi-key
 * command whose keys span shards (e.g. {@code MSET a 1 b 2}) is applied shard
 * by shard, exactly like a pipeline. Use a {hash tag} to keep keys that must
 * change together on one shard, which is also what Redis Cluster requires.
 *
 * <p>A batch belongs to one shard thread and is only used from it.
 */
public final class Batch {
    private static final Bytes SMEMBERS = Bytes.of("SMEMBERS");

    /** A sub-command's reply that will come from another shard. */
    private record Remote(int shard, int index) {}

    private enum MergeKind { SUM_INT, ALL_OK, CONCAT, SCAN, MGET, INTER, UNION, DIFF }

    /** Partial replies ({@link Frame} or {@link Remote}) combined into one. */
    private record Merge(Object[] parts, MergeKind kind, int[][] mgetPositions) {}

    private final Shard shard;
    private final Mode mode;
    /** One per reply: a {@link Frame}, raw {@link Bytes}, a {@link Remote} or a {@link Merge}. */
    private final List<Object> replies = new ArrayList<>();
    private List<Bytes[]>[] outbox;

    public Batch(Shard shard, Mode mode) {
        this.shard = shard;
        this.mode = mode;
    }

    /** Number of replies this batch will produce. */
    public int size() {
        return replies.size();
    }

    public boolean isEmpty() {
        return replies.isEmpty();
    }

    /** Queue a reply computed by the caller (keeps it in pipeline order). */
    public void reply(Frame f) {
        replies.add(f);
    }

    /** Queue pre-encoded reply bytes. */
    public void raw(Bytes b) {
        replies.add(b);
    }

    /** Plan one command: run it inline if it is local, queue it otherwise. */
    public void command(Bytes[] args) {
        String name = Commands.name(args[0]);
        Spec spec = Commands.lookup(name);
        if (spec == null) {
            reply(Commands.unknownCommand(args));
            return;
        }
        if (!Commands.arityOk(args.length, spec.arity())) {
            reply(Commands.wrongArgs(args));
            return;
        }
        if (spec.write() && mode == Mode.CLIENT && shard.router.repl.isReplica()) {
            reply(Frame.error("READONLY You can't write against a read only replica."));
            return;
        }
        int n = shard.router.n;
        Object pending = switch (spec.keys()) {
            case NONE -> shard.exec(args, mode, spec);
            case FIRST -> part(KeySlot.shardOf(args[1], n), args, spec);
            case GLOBAL -> {
                MergeKind kind = switch (name) {
                    case "DBSIZE" -> MergeKind.SUM_INT;
                    case "KEYS" -> MergeKind.CONCAT;
                    case "SCAN" -> MergeKind.SCAN;
                    default -> MergeKind.ALL_OK;
                };
                if (n == 1) {
                    yield part(0, args, spec);
                }
                Object[] parts = new Object[n];
                for (int s = 0; s < n; s++) {
                    parts[s] = part(s, args, spec);
                }
                yield new Merge(parts, kind, null);
            }
            case ALL -> multi(name, args, spec);
            case PAIRS -> pairs(args, spec);
        };
        replies.add(pending);
    }

    /**
     * Route one shard-level command. {@code spec} is passed when
     * {@code args[0]} is the looked-up command itself, to skip a second lookup.
     * Returns the reply ({@link Frame}) or where it will come from ({@link Remote}).
     */
    @SuppressWarnings("unchecked")
    private Object part(int target, Bytes[] args, Spec spec) {
        if (target == shard.idx) {
            return shard.exec(args, mode, spec);
        }
        if (outbox == null) {
            outbox = new List[shard.router.n];
        }
        List<Bytes[]> q = outbox[target];
        if (q == null) {
            q = new ArrayList<>();
            outbox[target] = q;
        }
        q.add(args);
        return new Remote(target, q.size() - 1);
    }

    /** Commands whose every argument is a key. */
    private Object multi(String name, Bytes[] args, Spec spec) {
        int n = shard.router.n;
        int first = KeySlot.shardOf(args[1], n);
        boolean sameShard = true;
        for (int i = 2; i < args.length && sameShard; i++) {
            sameShard = KeySlot.shardOf(args[i], n) == first;
        }
        if (sameShard) {
            return part(first, args, spec);
        }
        MergeKind setOp = switch (name) {
            case "SINTER" -> MergeKind.INTER;
            case "SUNION" -> MergeKind.UNION;
            case "SDIFF" -> MergeKind.DIFF;
            default -> null;
        };
        if (setOp != null) {
            // Fetch each set from its owner, combine here.
            Object[] parts = new Object[args.length - 1];
            for (int i = 1; i < args.length; i++) {
                parts[i - 1] = part(KeySlot.shardOf(args[i], n), new Bytes[] {SMEMBERS, args[i]}, null);
            }
            return new Merge(parts, setOp, null);
        }
        List<Integer> groupShard = new ArrayList<>();
        List<List<Bytes>> groupArgs = new ArrayList<>();
        int[][] positions = new int[args.length - 1][];
        for (int i = 1; i < args.length; i++) {
            int s = KeySlot.shardOf(args[i], n);
            int g = groupShard.indexOf(s);
            if (g < 0) {
                groupShard.add(s);
                List<Bytes> a = new ArrayList<>();
                a.add(args[0]);
                groupArgs.add(a);
                g = groupShard.size() - 1;
            }
            groupArgs.get(g).add(args[i]);
            positions[i - 1] = new int[] {g, groupArgs.get(g).size() - 2};
        }
        Object[] parts = new Object[groupShard.size()];
        for (int g = 0; g < parts.length; g++) {
            parts[g] = part(groupShard.get(g), groupArgs.get(g).toArray(Bytes[]::new), spec);
        }
        return name.equals("MGET")
                ? new Merge(parts, MergeKind.MGET, positions)
                : new Merge(parts, MergeKind.SUM_INT, null);
    }

    /** {@code MSET k v [k v ...]}. */
    private Object pairs(Bytes[] args, Spec spec) {
        if ((args.length - 1) % 2 != 0) {
            return Commands.wrongArgs(args);
        }
        int n = shard.router.n;
        List<Integer> groupShard = new ArrayList<>();
        List<List<Bytes>> groupArgs = new ArrayList<>();
        for (int i = 1; i < args.length; i += 2) {
            int s = KeySlot.shardOf(args[i], n);
            int g = groupShard.indexOf(s);
            if (g < 0) {
                groupShard.add(s);
                List<Bytes> a = new ArrayList<>();
                a.add(args[0]);
                groupArgs.add(a);
                g = groupShard.size() - 1;
            }
            groupArgs.get(g).add(args[i]);
            groupArgs.get(g).add(args[i + 1]);
        }
        if (groupShard.size() == 1) {
            return part(groupShard.get(0), args, spec);
        }
        Object[] parts = new Object[groupShard.size()];
        for (int g = 0; g < parts.length; g++) {
            parts[g] = part(groupShard.get(g), groupArgs.get(g).toArray(Bytes[]::new), spec);
        }
        return new Merge(parts, MergeKind.ALL_OK, null);
    }

    /**
     * Send queued sub-batches, wait for them and for durability, encode every
     * reply in request order into {@code out}, then run {@code done}. All on
     * this shard's thread; {@code done} runs synchronously when nothing has
     * to be waited for.
     */
    public void finish(ByteBuf out, Runnable done) {
        if (replies.isEmpty()) {
            done.run();
            return;
        }
        int n = shard.router.n;
        Frame[][] results = new Frame[n][];
        int[] outstanding = {0};
        long[] seq = {0};
        Runnable complete = () -> shard.whenDurable(seq[0], () -> {
            encode(out, results);
            done.run();
        });
        if (outbox != null) {
            for (int s = 0; s < n; s++) {
                List<Bytes[]> cmds = outbox[s];
                if (cmds == null) {
                    continue;
                }
                int target = s;
                boolean sent = shard.router.send(s, new ShardMsg.Exec(cmds, mode, shard, frames -> {
                    results[target] = frames;
                    if (--outstanding[0] == 0) {
                        complete.run();
                    }
                }));
                if (sent) {
                    outstanding[0]++;
                }
            }
        }
        // Our own inline writes go to our AOF while the other shards work.
        seq[0] = shard.commit();
        if (outstanding[0] == 0) {
            complete.run();
        }
    }

    private static Frame take(Frame[][] results, Remote r) {
        Frame[] fs = results[r.shard];
        if (fs == null || r.index >= fs.length || fs[r.index] == null) {
            return Frame.error("ERR shard unavailable");
        }
        Frame f = fs[r.index];
        fs[r.index] = null;
        return f;
    }

    private static Frame resolve(Object part, Frame[][] results) {
        return part instanceof Remote r ? take(results, r) : (Frame) part;
    }

    private void encode(ByteBuf out, Frame[][] results) {
        for (Object p : replies) {
            switch (p) {
                case Frame f -> f.encode(out);
                case Bytes b -> out.put(b);
                case Remote r -> take(results, r).encode(out);
                case Merge m -> {
                    Frame[] frames = new Frame[m.parts.length];
                    for (int i = 0; i < frames.length; i++) {
                        frames[i] = resolve(m.parts[i], results);
                    }
                    merge(frames, m).encode(out);
                }
                default -> throw new IllegalStateException("unexpected pending reply " + p);
            }
        }
    }

    private static List<Frame> items(Frame f) {
        return f instanceof Frame.Array a ? a.items() : List.of();
    }

    private static Frame merge(Frame[] frames, Merge m) {
        for (Frame f : frames) {
            if (f instanceof Frame.Error) {
                return f;
            }
        }
        switch (m.kind) {
            case SUM_INT: {
                long sum = 0;
                for (Frame f : frames) {
                    if (f instanceof Frame.Int i) {
                        sum += i.value();
                    }
                }
                return Frame.integer(sum);
            }
            case ALL_OK:
                return Frame.OK;
            case CONCAT: {
                List<Frame> all = new ArrayList<>();
                for (Frame f : frames) {
                    all.addAll(items(f));
                }
                return Frame.array(all);
            }
            case SCAN: {
                List<Frame> keys = new ArrayList<>();
                for (Frame f : frames) {
                    List<Frame> parts = items(f);
                    if (parts.size() == 2) {
                        keys.addAll(items(parts.get(1)));
                    }
                }
                return Frame.array(Frame.bulk("0"), Frame.array(keys));
            }
            case MGET: {
                List<List<Frame>> parts = new ArrayList<>();
                for (Frame f : frames) {
                    parts.add(items(f));
                }
                List<Frame> out = new ArrayList<>(m.mgetPositions.length);
                for (int[] pos : m.mgetPositions) {
                    List<Frame> a = pos[0] < parts.size() ? parts.get(pos[0]) : List.of();
                    out.add(pos[1] < a.size() ? a.get(pos[1]) : Frame.NULL);
                }
                return Frame.array(out);
            }
            default: { // set algebra
                HashSet<Bytes> acc = null;
                for (Frame f : frames) {
                    HashSet<Bytes> s = new HashSet<>();
                    for (Frame i : items(f)) {
                        if (i instanceof Frame.Bulk b) {
                            s.add(b.value());
                        }
                    }
                    if (acc == null) {
                        acc = s;
                    } else if (m.kind == MergeKind.INTER) {
                        acc.retainAll(s);
                    } else if (m.kind == MergeKind.UNION) {
                        acc.addAll(s);
                    } else {
                        acc.removeAll(s);
                    }
                }
                List<Frame> out = new ArrayList<>();
                if (acc != null) {
                    for (Bytes b : acc) {
                        out.add(new Frame.Bulk(b));
                    }
                }
                return Frame.array(out);
            }
        }
    }
}
