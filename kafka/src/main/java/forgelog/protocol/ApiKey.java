package forgelog.protocol;

/** Request types (the {@code int16 apiKey} of a request frame). */
public final class ApiKey {
    public static final short METADATA = 0;
    public static final short CREATE_TOPIC = 1;
    public static final short PRODUCE = 2;
    public static final short FETCH = 3;
    public static final short JOIN_GROUP = 4;
    public static final short HEARTBEAT = 5;
    public static final short LEAVE_GROUP = 6;
    public static final short OFFSET_COMMIT = 7;
    public static final short OFFSET_FETCH = 8;
    /** Broker-to-broker Raft message. One-way: never answered. */
    public static final short RAFT = 9;
    /** Per-broker view of every Raft group (roles, terms, commit indexes). */
    public static final short STATUS = 10;
    /** Fault injection: drop all traffic to and from the given brokers. */
    public static final short CHAOS = 11;

    private ApiKey() {}

    public static String name(short key) {
        return switch (key) {
            case METADATA -> "METADATA";
            case CREATE_TOPIC -> "CREATE_TOPIC";
            case PRODUCE -> "PRODUCE";
            case FETCH -> "FETCH";
            case JOIN_GROUP -> "JOIN_GROUP";
            case HEARTBEAT -> "HEARTBEAT";
            case LEAVE_GROUP -> "LEAVE_GROUP";
            case OFFSET_COMMIT -> "OFFSET_COMMIT";
            case OFFSET_FETCH -> "OFFSET_FETCH";
            case RAFT -> "RAFT";
            case STATUS -> "STATUS";
            case CHAOS -> "CHAOS";
            default -> "UNKNOWN(" + key + ")";
        };
    }
}
