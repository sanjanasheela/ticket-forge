package forgelog.broker;

import forgelog.log.CommitLog;
import forgelog.log.Record;
import forgelog.protocol.Errors;
import forgelog.protocol.Messages.GroupStatus;
import forgelog.raft.NotLeaderException;
import forgelog.raft.RaftNode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * This broker's replica of one Raft group, backed by a {@link CommitLog}: the
 * metadata group, the consumer-offsets group, or a topic partition.
 *
 * <p>Committed entries are applied in order ({@link #apply}); proposals
 * register callbacks that fire once their index commits, or fail if this
 * replica loses leadership first (the entry may still commit later under the
 * next leader, which is why producers are idempotent).
 */
abstract class Replica implements RaftNode.Listener {
    private record Waiter(Runnable ok, Consumer<Errors> fail) {}

    final Broker broker;
    final int groupId;
    final String name;
    final CommitLog log;
    final FileHardState hardState;
    RaftNode raft;
    long lastApplied = -1;
    private long hintWritten = -1;
    private final TreeMap<Long, List<Waiter>> waiters = new TreeMap<>();

    Replica(Broker broker, int groupId, String name, Path dir) throws IOException {
        this.broker = broker;
        this.groupId = groupId;
        this.name = name;
        this.log = CommitLog.open(dir, broker.config().log());
        this.hardState = FileHardState.open(dir);
    }

    /** Create the Raft node and replay what is known to be committed. */
    void start(long now) {
        long hint = hardState.readCommitHint();
        BrokerConfig cfg = broker.config();
        raft = new RaftNode(groupId, cfg.nodeId(), cfg.members(), log, hardState, broker::sendRaft, this,
                cfg.timing(), new Random(System.nanoTime() ^ ((long) cfg.nodeId() << 32) ^ groupId), now, hint);
        applyCommitted();
    }

    /** Apply one committed data entry (in order, exactly once per process lifetime). */
    void apply(Record r) {}

    /** Whether {@link #apply} needs the entries at all (partitions don't). */
    boolean needsApply() {
        return true;
    }

    void applyCommitted() {
        long commit = raft.commitIndex();
        if (!needsApply()) {
            lastApplied = Math.max(lastApplied, commit);
            return;
        }
        while (lastApplied < commit) {
            for (Record r : log.entries(lastApplied + 1, commit, 1 << 20)) {
                if (r.isData()) {
                    apply(r);
                }
                lastApplied = r.offset;
            }
        }
    }

    long propose(Record r) throws NotLeaderException {
        return raft.propose(r);
    }

    /** Run {@code ok} once {@code index} is committed, or {@code fail} if leadership is lost first. */
    void whenCommitted(long index, Runnable ok, Consumer<Errors> fail) {
        if (raft.commitIndex() >= index) {
            ok.run();
            return;
        }
        waiters.computeIfAbsent(index, k -> new ArrayList<>(2)).add(new Waiter(ok, fail));
    }

    @Override
    public void onCommit(long commitIndex) {
        applyCommitted();
        while (!waiters.isEmpty() && waiters.firstKey() <= commitIndex) {
            for (Waiter w : waiters.pollFirstEntry().getValue()) {
                w.ok.run();
            }
        }
        committed(commitIndex);
    }

    /** Hook: entries up to {@code commitIndex} are committed and applied. */
    void committed(long commitIndex) {}

    @Override
    public void onRoleChange(RaftNode.Role role, long term, int leaderId) {
        if (role != RaftNode.Role.LEADER && !waiters.isEmpty()) {
            List<Waiter> failed = new ArrayList<>();
            for (Map.Entry<Long, List<Waiter>> e : waiters.entrySet()) {
                failed.addAll(e.getValue());
            }
            waiters.clear();
            for (Waiter w : failed) {
                w.fail.accept(Errors.NOT_LEADER);
            }
        }
        broker.onRoleChange(this, role, term, leaderId);
    }

    void maybeWriteHint() {
        long c = raft.commitIndex();
        if (c != hintWritten) {
            hardState.writeCommitHint(c);
            hintWritten = c;
        }
    }

    GroupStatus status() {
        return new GroupStatus(groupId, name, raft.role().name(), raft.term(), raft.leaderId(), raft.lastIndex(),
                raft.commitIndex());
    }

    void close() {
        try {
            log.flush(true);
            log.close();
            if (hardState.isDirty()) {
                hardState.flush();
            }
        } catch (IOException e) {
            // shutting down
        }
        maybeWriteHint();
    }
}
