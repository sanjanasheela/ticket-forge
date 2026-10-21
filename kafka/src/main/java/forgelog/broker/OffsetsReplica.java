package forgelog.broker;

import forgelog.log.Record;
import forgelog.protocol.Messages.PartitionOffset;
import forgelog.protocol.TopicPartition;
import forgelog.protocol.Wire;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Committed consumer-group offsets: Raft group 1, replicated on every broker
 * (the equivalent of {@code __consumer_offsets}). Its leader is the group
 * coordinator. An offset commit is acknowledged only once a quorum has it,
 * so committed positions survive the loss of the coordinator.
 */
final class OffsetsReplica extends Replica {
    static final int GROUP_ID = 1;

    private final Map<String, Map<TopicPartition, Long>> committed = new HashMap<>();

    OffsetsReplica(Broker broker, Path dir) throws IOException {
        super(broker, GROUP_ID, "__offsets", dir);
    }

    static Record commit(String group, List<PartitionOffset> offsets) {
        Wire.Writer w = new Wire.Writer(64 + offsets.size() * 32);
        w.str(group).i32(offsets.size());
        for (PartitionOffset o : offsets) {
            w.str(o.topic()).i32(o.partition()).i64(o.offset());
        }
        ByteBuffer b = w.toBuffer();
        byte[] v = new byte[b.remaining()];
        b.get(v);
        return Record.data(v);
    }

    @Override
    void apply(Record r) {
        Wire.Reader in = new Wire.Reader(ByteBuffer.wrap(r.value));
        Map<TopicPartition, Long> g = committed.computeIfAbsent(in.str(), k -> new HashMap<>());
        int n = in.i32();
        for (int i = 0; i < n; i++) {
            g.put(new TopicPartition(in.str(), in.i32()), in.i64());
        }
    }

    /** The committed offset, or -1. */
    long offset(String group, TopicPartition tp) {
        Map<TopicPartition, Long> g = committed.get(group);
        Long o = g == null ? null : g.get(tp);
        return o == null ? -1 : o;
    }
}
