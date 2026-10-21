package forgelog.raft;

/** A proposal reached a replica that is not the leader. */
public final class NotLeaderException extends Exception {
    /** The leader this replica knows of, or -1. */
    public final int leaderHint;

    public NotLeaderException(int leaderHint) {
        super("not the leader (leader is " + leaderHint + ")", null, false, false);
        this.leaderHint = leaderHint;
    }
}
