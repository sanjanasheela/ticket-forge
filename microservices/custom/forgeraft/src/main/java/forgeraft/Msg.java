package forgeraft;

import java.io.Serializable;
import java.util.List;

public interface Msg extends Serializable {
    record Entry(long term, byte[] data) implements Serializable {}
    record RequestVote(long term, String candidate, long lastIndex, long lastTerm) implements Msg {}
    record VoteResp(long term, boolean granted) implements Msg {}
    record Append(long term, String leader, long prevIndex, long prevTerm, List<Entry> entries, long leaderCommit) implements Msg {}
    record AppendResp(long term, boolean success, long matchIndex) implements Msg {}
}
