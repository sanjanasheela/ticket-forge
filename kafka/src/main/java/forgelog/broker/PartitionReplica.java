package forgelog.broker;

import forgelog.log.CommitLog;
import forgelog.log.Record;
import forgelog.protocol.TopicPartition;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * One topic partition: a Raft group whose log <em>is</em> the partition log
 * (offset == Raft index). Consumers read up to the high watermark, which is
 * the Raft commit index + 1, so they only ever see records a quorum has.
 *
 * <p>It also tracks, per idempotent producer, the sequence numbers of its
 * latest records and where they landed. A retried send whose sequence is
 * already in the log (even uncommitted, e.g. written just before a leader
 * change) is answered with the existing offset instead of being appended
 * again. The state is maintained on every replica as the log changes,
 * including truncation, so a new leader has it immediately.
 */
final class PartitionReplica extends Replica implements CommitLog.Listener {
    /** Recent (sequence, offset) pairs kept per producer. */
    private static final int PRODUCER_HISTORY = 16;
    /** Records scanned at startup to rebuild producer state. */
    private static final int PRODUCER_SCAN = 100_000;

    final TopicPartition tp;
    private final Map<Long, ArrayDeque<long[]>> producers = new HashMap<>();

    PartitionReplica(Broker broker, int groupId, TopicPartition tp, Path dir) throws IOException {
        super(broker, groupId, tp.toString(), dir);
        this.tp = tp;
        long from = Math.max(0, log.lastIndex() - PRODUCER_SCAN + 1);
        while (from <= log.lastIndex()) {
            for (Record r : log.entries(from, log.lastIndex(), 4 << 20)) {
                onAppend(r);
                from = r.offset + 1;
            }
        }
        log.setListener(this);
    }

    @Override
    boolean needsApply() {
        return false;
    }

    /** Next offset to become readable: everything below it is committed. */
    long highWatermark() {
        return raft.commitIndex() + 1;
    }

    @Override
    void committed(long commitIndex) {
        broker.wakeFetchers(this);
    }

    @Override
    public void onAppend(Record r) {
        if (r.producerId < 0 || !r.isData()) {
            return;
        }
        ArrayDeque<long[]> h = producers.computeIfAbsent(r.producerId, k -> new ArrayDeque<>());
        h.addLast(new long[] {r.sequence, r.offset});
        if (h.size() > PRODUCER_HISTORY) {
            h.removeFirst();
        }
    }

    @Override
    public void onTruncate(long fromOffset) {
        for (Iterator<ArrayDeque<long[]>> it = producers.values().iterator(); it.hasNext();) {
            ArrayDeque<long[]> h = it.next();
            while (!h.isEmpty() && h.peekLast()[1] >= fromOffset) {
                h.removeLast();
            }
            if (h.isEmpty()) {
                it.remove();
            }
        }
    }

    /** Last sequence number in the log for {@code producerId}, or {@code null} if unknown. */
    Integer lastSequence(long producerId) {
        ArrayDeque<long[]> h = producers.get(producerId);
        return h == null ? null : (int) h.peekLast()[0];
    }

    /** Offset of an already-appended record, or -1 if it is older than the history. */
    long offsetOf(long producerId, int sequence) {
        ArrayDeque<long[]> h = producers.get(producerId);
        if (h != null) {
            for (long[] e : h) {
                if (e[0] == sequence) {
                    return e[1];
                }
            }
        }
        return -1;
    }
}
