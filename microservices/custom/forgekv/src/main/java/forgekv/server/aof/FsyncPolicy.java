package forgekv.server.aof;

import java.util.Locale;

/** When to fsync the AOF, as in Redis' {@code appendfsync}. */
public enum FsyncPolicy {
    ALWAYS,
    EVERYSEC,
    NO;

    public static FsyncPolicy parse(String s) {
        return switch (s.toLowerCase(Locale.ROOT)) {
            case "always" -> ALWAYS;
            case "everysec" -> EVERYSEC;
            case "no" -> NO;
            default -> throw new IllegalArgumentException(
                    "invalid appendfsync policy '" + s.toLowerCase(Locale.ROOT) + "' (expected always|everysec|no)");
        };
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
