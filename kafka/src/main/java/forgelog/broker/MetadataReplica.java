package forgelog.broker;

import forgelog.log.Record;
import forgelog.protocol.Wire;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The cluster metadata: Raft group 0, replicated on every broker (the
 * equivalent of KRaft's {@code __cluster_metadata}). Its leader is the
 * controller. Entries are commands; applying {@code CREATE_TOPIC} on every
 * broker, in the same order, gives every partition the same Raft group id
 * everywhere without any further coordination.
 */
final class MetadataReplica extends Replica {
    static final int GROUP_ID = 0;
    /** Group ids 0 and 1 are the metadata and offsets groups. */
    static final int FIRST_PARTITION_GROUP = 2;

    private static final byte CREATE_TOPIC = 1;

    /** A topic and the Raft group ids of its partitions ({@code firstGroupId + partition}). */
    record TopicMeta(String name, int partitions, int firstGroupId) {}

    private final Map<String, TopicMeta> topics = new LinkedHashMap<>();
    private int nextGroupId = FIRST_PARTITION_GROUP;

    MetadataReplica(Broker broker, Path dir) throws IOException {
        super(broker, GROUP_ID, "__metadata", dir);
    }

    Map<String, TopicMeta> topics() {
        return topics;
    }

    static Record createTopic(String name, int partitions) {
        Wire.Writer w = new Wire.Writer(64);
        w.i8(CREATE_TOPIC).str(name).i32(partitions);
        ByteBuffer b = w.toBuffer();
        byte[] v = new byte[b.remaining()];
        b.get(v);
        return Record.data(v);
    }

    @Override
    void apply(Record r) {
        Wire.Reader in = new Wire.Reader(ByteBuffer.wrap(r.value));
        byte op = in.i8();
        if (op == CREATE_TOPIC) {
            String name = in.str();
            int partitions = in.i32();
            if (!topics.containsKey(name)) {
                TopicMeta t = new TopicMeta(name, partitions, nextGroupId);
                nextGroupId += partitions;
                topics.put(name, t);
                broker.onTopicCreated(t);
            }
        }
    }
}
