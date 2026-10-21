package forgekv.server.aof;

import java.util.Locale;

/** Which syscall interface the AOF writer uses. */
public enum IoBackend {
    /** io_uring when the kernel allows it, std otherwise. */
    AUTO,
    URING,
    STD;

    public static IoBackend parse(String s) {
        return switch (s.toLowerCase(Locale.ROOT)) {
            case "auto" -> AUTO;
            case "uring", "io_uring", "io-uring" -> URING;
            case "std" -> STD;
            default -> throw new IllegalArgumentException(
                    "invalid io backend '" + s.toLowerCase(Locale.ROOT) + "' (expected auto|uring|std)");
        };
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
