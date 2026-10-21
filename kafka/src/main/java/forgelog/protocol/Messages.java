package forgelog.protocol;

import forgelog.protocol.Wire.Reader;
import forgelog.protocol.Wire.Writer;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Request and response bodies, shared by the broker and the clients. Each
 * record encodes itself and has a matching static {@code decode}.
 */
public final class Messages {
    private Messages() {}

    private static Errors err(Reader r) {
        return Errors.of(r.i16());
    }

    // -----------------------------------------------------------------------
    // shared
    // -----------------------------------------------------------------------

    /** The answer to most coordination requests: an error plus a hint at who to ask instead. */
    public record SimpleResponse(Errors error, int leaderHint) {
        public void encode(Writer w) {
            w.i16(error.code).i32(leaderHint);
        }

        public static SimpleResponse decode(Reader r) {
            return new SimpleResponse(err(r), r.i32());
        }
    }

    // -----------------------------------------------------------------------
    // METADATA
    // -----------------------------------------------------------------------

    public record BrokerInfo(int id, String host, int port) {}

    /** {@code leader} as this broker knows it (-1 = none yet). */
    public record PartitionInfo(int partition, int leader, long highWatermark) {}

    public record TopicInfo(String name, Errors error, List<PartitionInfo> partitions) {}

    /** Empty {@code topics} = every topic. */
    public record MetadataRequest(List<String> topics) {
        public void encode(Writer w) {
            w.strings(topics);
        }

        public static MetadataRequest decode(Reader r) {
            return new MetadataRequest(r.strings());
        }
    }

    /**
     * @param controller leader of the metadata Raft group (topic creation goes there)
     * @param coordinator leader of the offsets Raft group (the group coordinator)
     */
    public record MetadataResponse(int nodeId, List<BrokerInfo> brokers, int controller, int coordinator,
            List<TopicInfo> topics) {
        public void encode(Writer w) {
            w.i32(nodeId).i32(brokers.size());
            for (BrokerInfo b : brokers) {
                w.i32(b.id()).str(b.host()).i32(b.port());
            }
            w.i32(controller).i32(coordinator).i32(topics.size());
            for (TopicInfo t : topics) {
                w.str(t.name()).i16(t.error().code).i32(t.partitions().size());
                for (PartitionInfo p : t.partitions()) {
                    w.i32(p.partition()).i32(p.leader()).i64(p.highWatermark());
                }
            }
        }

        public static MetadataResponse decode(Reader r) {
            int node = r.i32();
            List<BrokerInfo> brokers = r.list(x -> new BrokerInfo(x.i32(), x.str(), x.i32()));
            int controller = r.i32();
            int coordinator = r.i32();
            List<TopicInfo> topics = r.list(x -> new TopicInfo(x.str(), err(x),
                    x.list(y -> new PartitionInfo(y.i32(), y.i32(), y.i64()))));
            return new MetadataResponse(node, brokers, controller, coordinator, topics);
        }
    }

    // -----------------------------------------------------------------------
    // CREATE_TOPIC (answered with SimpleResponse)
    // -----------------------------------------------------------------------

    public record CreateTopicRequest(String name, int partitions) {
        public void encode(Writer w) {
            w.str(name).i32(partitions);
        }

        public static CreateTopicRequest decode(Reader r) {
            return new CreateTopicRequest(r.str(), r.i32());
        }
    }

    // -----------------------------------------------------------------------
    // PRODUCE
    // -----------------------------------------------------------------------

    /** {@code producerId}/{@code sequence} = -1: not idempotent. */
    public record ProduceRecord(long producerId, int sequence, long timestamp, byte[] key, byte[] value) {}

    public record ProducePartition(String topic, int partition, List<ProduceRecord> records) {}

    public record ProduceRequest(int timeoutMs, List<ProducePartition> partitions) {
        public void encode(Writer w) {
            w.i32(timeoutMs).i32(partitions.size());
            for (ProducePartition p : partitions) {
                w.str(p.topic()).i32(p.partition()).i32(p.records().size());
                for (ProduceRecord r : p.records()) {
                    w.i64(r.producerId()).i32(r.sequence()).i64(r.timestamp()).bytes(r.key()).bytes(r.value());
                }
            }
        }

        public static ProduceRequest decode(Reader r) {
            int timeout = r.i32();
            List<ProducePartition> parts = r.list(x -> new ProducePartition(x.str(), x.i32(),
                    x.list(y -> new ProduceRecord(y.i64(), y.i32(), y.i64(), y.bytes(), y.bytes()))));
            return new ProduceRequest(timeout, parts);
        }
    }

    /** {@code offsets}: one per record, in request order (only meaningful when {@code error == NONE}). */
    public record ProduceResult(String topic, int partition, Errors error, int leaderHint, long[] offsets) {}

    public record ProduceResponse(List<ProduceResult> results) {
        public void encode(Writer w) {
            w.i32(results.size());
            for (ProduceResult p : results) {
                w.str(p.topic()).i32(p.partition()).i16(p.error().code).i32(p.leaderHint()).i32(p.offsets().length);
                for (long o : p.offsets()) {
                    w.i64(o);
                }
            }
        }

        public static ProduceResponse decode(Reader r) {
            return new ProduceResponse(r.list(x -> {
                String t = x.str();
                int p = x.i32();
                Errors e = err(x);
                int hint = x.i32();
                int n = x.count();
                long[] offs = new long[n];
                for (int i = 0; i < n; i++) {
                    offs[i] = x.i64();
                }
                return new ProduceResult(t, p, e, hint, offs);
            }));
        }
    }

    // -----------------------------------------------------------------------
    // FETCH
    // -----------------------------------------------------------------------

    public record FetchPartition(String topic, int partition, long offset, int maxBytes) {}

    /** Waits up to {@code maxWaitMs} for data if none of the partitions has any. */
    public record FetchRequest(int maxWaitMs, List<FetchPartition> partitions) {
        public void encode(Writer w) {
            w.i32(maxWaitMs).i32(partitions.size());
            for (FetchPartition p : partitions) {
                w.str(p.topic()).i32(p.partition()).i64(p.offset()).i32(p.maxBytes());
            }
        }

        public static FetchRequest decode(Reader r) {
            int wait = r.i32();
            return new FetchRequest(wait, r.list(x -> new FetchPartition(x.str(), x.i32(), x.i64(), x.i32())));
        }
    }

    /**
     * {@code records}: raw, encoded log records exactly as stored (control
     * records included); {@code highWatermark}: next offset to be committed.
     */
    public record FetchResult(String topic, int partition, Errors error, int leaderHint, long highWatermark,
            ByteBuffer records) {
        /** Everything but the record bytes, which the broker sends straight from the file. */
        public void encodeHeader(Writer w, int recordBytes) {
            w.str(topic).i32(partition).i16(error.code).i32(leaderHint).i64(highWatermark).i32(recordBytes);
        }
    }

    public record FetchResponse(List<FetchResult> results) {
        public static FetchResponse decode(Reader r) {
            return new FetchResponse(r.list(x -> {
                String t = x.str();
                int p = x.i32();
                Errors e = err(x);
                int hint = x.i32();
                long hw = x.i64();
                ByteBuffer recs = x.slice(x.i32());
                return new FetchResult(t, p, e, hint, hw, recs);
            }));
        }
    }

    // -----------------------------------------------------------------------
    // consumer groups
    // -----------------------------------------------------------------------

    /** {@code memberId} is empty on the first join. */
    public record JoinGroupRequest(String group, String memberId, int sessionTimeoutMs, List<String> topics) {
        public void encode(Writer w) {
            w.str(group).str(memberId).i32(sessionTimeoutMs).strings(topics);
        }

        public static JoinGroupRequest decode(Reader r) {
            return new JoinGroupRequest(r.str(), r.str(), r.i32(), r.strings());
        }
    }

    public record JoinGroupResponse(Errors error, int coordinatorHint, int generation, String memberId,
            List<TopicPartition> assignment) {
        public void encode(Writer w) {
            w.i16(error.code).i32(coordinatorHint).i32(generation).str(memberId).i32(assignment.size());
            for (TopicPartition tp : assignment) {
                w.str(tp.topic()).i32(tp.partition());
            }
        }

        public static JoinGroupResponse decode(Reader r) {
            return new JoinGroupResponse(err(r), r.i32(), r.i32(), r.str(),
                    r.list(x -> new TopicPartition(x.str(), x.i32())));
        }
    }

    public record HeartbeatRequest(String group, String memberId, int generation) {
        public void encode(Writer w) {
            w.str(group).str(memberId).i32(generation);
        }

        public static HeartbeatRequest decode(Reader r) {
            return new HeartbeatRequest(r.str(), r.str(), r.i32());
        }
    }

    public record LeaveGroupRequest(String group, String memberId) {
        public void encode(Writer w) {
            w.str(group).str(memberId);
        }

        public static LeaveGroupRequest decode(Reader r) {
            return new LeaveGroupRequest(r.str(), r.str());
        }
    }

    /** {@code offset}: the next offset to consume (-1 in a fetch = nothing committed). */
    public record PartitionOffset(String topic, int partition, long offset) {}

    private static void encodeOffsets(Writer w, List<PartitionOffset> offsets) {
        w.i32(offsets.size());
        for (PartitionOffset o : offsets) {
            w.str(o.topic()).i32(o.partition()).i64(o.offset());
        }
    }

    private static List<PartitionOffset> decodeOffsets(Reader r) {
        return r.list(x -> new PartitionOffset(x.str(), x.i32(), x.i64()));
    }

    /**
     * {@code generation} = -1 with an empty {@code memberId}: a standalone
     * consumer that manages its own assignment (only allowed for empty groups).
     */
    public record OffsetCommitRequest(String group, String memberId, int generation, List<PartitionOffset> offsets) {
        public void encode(Writer w) {
            w.str(group).str(memberId).i32(generation);
            encodeOffsets(w, offsets);
        }

        public static OffsetCommitRequest decode(Reader r) {
            return new OffsetCommitRequest(r.str(), r.str(), r.i32(), decodeOffsets(r));
        }
    }

    public record OffsetFetchRequest(String group, List<TopicPartition> partitions) {
        public void encode(Writer w) {
            w.str(group).i32(partitions.size());
            for (TopicPartition tp : partitions) {
                w.str(tp.topic()).i32(tp.partition());
            }
        }

        public static OffsetFetchRequest decode(Reader r) {
            return new OffsetFetchRequest(r.str(), r.list(x -> new TopicPartition(x.str(), x.i32())));
        }
    }

    public record OffsetFetchResponse(Errors error, int coordinatorHint, List<PartitionOffset> offsets) {
        public void encode(Writer w) {
            w.i16(error.code).i32(coordinatorHint);
            encodeOffsets(w, offsets);
        }

        public static OffsetFetchResponse decode(Reader r) {
            return new OffsetFetchResponse(err(r), r.i32(), decodeOffsets(r));
        }
    }

    // -----------------------------------------------------------------------
    // STATUS / CHAOS
    // -----------------------------------------------------------------------

    public record GroupStatus(int group, String name, String role, long term, int leader, long lastIndex,
            long commitIndex) {}

    public record StatusResponse(int nodeId, int[] blocked, List<GroupStatus> groups) {
        public void encode(Writer w) {
            w.i32(nodeId).ints(blocked).i32(groups.size());
            for (GroupStatus g : groups) {
                w.i32(g.group()).str(g.name()).str(g.role()).i64(g.term()).i32(g.leader()).i64(g.lastIndex())
                        .i64(g.commitIndex());
            }
        }

        public static StatusResponse decode(Reader r) {
            int node = r.i32();
            int[] blocked = r.ints();
            return new StatusResponse(node, blocked, r.list(x -> new GroupStatus(x.i32(), x.str(), x.str(), x.i64(),
                    x.i32(), x.i64(), x.i64())));
        }
    }

    /** Replace the set of brokers this broker drops all traffic to and from. */
    public record ChaosRequest(int[] blockedPeers) {
        public void encode(Writer w) {
            w.ints(blockedPeers);
        }

        public static ChaosRequest decode(Reader r) {
            return new ChaosRequest(r.ints());
        }
    }
}
