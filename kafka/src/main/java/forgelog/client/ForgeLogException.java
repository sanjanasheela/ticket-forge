package forgelog.client;

import forgelog.protocol.Errors;

/** A request the client could not complete. */
public class ForgeLogException extends RuntimeException {
    public final Errors error;

    public ForgeLogException(String message) {
        this(message, null, null);
    }

    public ForgeLogException(Errors error, String message) {
        this(message, error, null);
    }

    public ForgeLogException(String message, Errors error, Throwable cause) {
        super(message, cause);
        this.error = error;
    }

    /** An offset commit was refused because the group rebalanced; rejoin and retry. */
    public static final class CommitFailedException extends ForgeLogException {
        public CommitFailedException(Errors error) {
            super(error, "offset commit refused (" + error + "): the group rebalanced, partitions may have moved");
        }
    }
}
