package forgelog.raft;

import forgelog.log.Record;
import java.util.List;

/** Messages between the replicas of one Raft group. */
public sealed interface RaftMessage {
    int group();

    int from();

    long term();

    /**
     * {@code preVote}: a probe that does not change anyone's term (Raft thesis
     * §9.6). {@code term} is then the term the sender <em>would</em> use.
     */
    record VoteRequest(int group, int from, long term, long lastIndex, long lastTerm, boolean preVote)
            implements RaftMessage {}

    record VoteResponse(int group, int from, long term, boolean granted, boolean preVote) implements RaftMessage {}

    /** Also the heartbeat (no entries). */
    record AppendRequest(int group, int from, long term, long prevIndex, long prevTerm, long leaderCommit,
            List<Record> entries) implements RaftMessage {}

    /**
     * {@code matchIndex}: on success, the last index known to match the leader.
     * {@code hintIndex}: on failure, where the leader should retry from.
     */
    record AppendResponse(int group, int from, long term, boolean success, long matchIndex, long hintIndex)
            implements RaftMessage {}
}
