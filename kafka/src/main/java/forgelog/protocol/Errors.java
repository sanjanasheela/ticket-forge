package forgelog.protocol;

/** Error codes carried in responses. */
public enum Errors {
    NONE(0, false),
    UNKNOWN_TOPIC_OR_PARTITION(1, true),
    NOT_LEADER(2, true),
    TOPIC_ALREADY_EXISTS(3, false),
    INVALID_REQUEST(4, false),
    OFFSET_OUT_OF_RANGE(5, false),
    NOT_COORDINATOR(6, true),
    REBALANCE_IN_PROGRESS(7, false),
    UNKNOWN_MEMBER_ID(8, false),
    ILLEGAL_GENERATION(9, false),
    OUT_OF_ORDER_SEQUENCE(10, false),
    REQUEST_TIMED_OUT(11, true),
    /** A leader exists but is not ready yet (its term's first entry is not committed). */
    LEADER_NOT_AVAILABLE(12, true),
    STORAGE_ERROR(13, false),
    INVALID_TOPIC(14, false);

    public final short code;
    /** Worth retrying (after refreshing metadata / backing off). */
    public final boolean retriable;

    Errors(int code, boolean retriable) {
        this.code = (short) code;
        this.retriable = retriable;
    }

    private static final Errors[] BY_CODE = values();

    public static Errors of(short code) {
        if (code < 0 || code >= BY_CODE.length) {
            throw new Wire.ProtocolException("unknown error code " + code);
        }
        return BY_CODE[code];
    }
}
