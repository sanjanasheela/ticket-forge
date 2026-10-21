package forgelog.protocol;

import forgelog.log.CorruptRecordException;
import forgelog.log.Record;
import forgelog.protocol.Wire.Reader;
import forgelog.protocol.Wire.Writer;
import forgelog.raft.RaftMessage;
import forgelog.raft.RaftMessage.AppendRequest;
import forgelog.raft.RaftMessage.AppendResponse;
import forgelog.raft.RaftMessage.VoteRequest;
import forgelog.raft.RaftMessage.VoteResponse;
import java.util.ArrayList;
import java.util.List;

/** Binary form of {@link RaftMessage}s (the body of a {@link ApiKey#RAFT} frame). */
public final class RaftCodec {
    private static final byte VOTE_REQ = 1;
    private static final byte VOTE_RESP = 2;
    private static final byte APPEND_REQ = 3;
    private static final byte APPEND_RESP = 4;

    private RaftCodec() {}

    public static void encode(RaftMessage m, Writer w) {
        switch (m) {
            case VoteRequest v -> w.i8(VOTE_REQ).i32(v.group()).i32(v.from()).i64(v.term()).i64(v.lastIndex())
                    .i64(v.lastTerm()).bool(v.preVote());
            case VoteResponse v -> w.i8(VOTE_RESP).i32(v.group()).i32(v.from()).i64(v.term()).bool(v.granted())
                    .bool(v.preVote());
            case AppendRequest a -> {
                w.i8(APPEND_REQ).i32(a.group()).i32(a.from()).i64(a.term()).i64(a.prevIndex()).i64(a.prevTerm())
                        .i64(a.leaderCommit()).i32(a.entries().size());
                for (Record r : a.entries()) {
                    r.encode(w.ensure(r.encodedSize()));
                }
            }
            case AppendResponse a -> w.i8(APPEND_RESP).i32(a.group()).i32(a.from()).i64(a.term()).bool(a.success())
                    .i64(a.matchIndex()).i64(a.hintIndex());
        }
    }

    public static RaftMessage decode(Reader r) {
        byte type = r.i8();
        int group = r.i32();
        int from = r.i32();
        long term = r.i64();
        return switch (type) {
            case VOTE_REQ -> new VoteRequest(group, from, term, r.i64(), r.i64(), r.bool());
            case VOTE_RESP -> new VoteResponse(group, from, term, r.bool(), r.bool());
            case APPEND_REQ -> {
                long prevIndex = r.i64();
                long prevTerm = r.i64();
                long commit = r.i64();
                int n = r.count();
                List<Record> entries = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    try {
                        Record rec = Record.decode(r.buffer());
                        if (rec == null) {
                            throw new Wire.ProtocolException("truncated entry");
                        }
                        entries.add(rec);
                    } catch (CorruptRecordException e) {
                        throw new Wire.ProtocolException("corrupt entry: " + e.getMessage());
                    }
                }
                yield new AppendRequest(group, from, term, prevIndex, prevTerm, commit, entries);
            }
            case APPEND_RESP -> new AppendResponse(group, from, term, r.bool(), r.i64(), r.i64());
            default -> throw new Wire.ProtocolException("unknown raft message type " + type);
        };
    }
}
