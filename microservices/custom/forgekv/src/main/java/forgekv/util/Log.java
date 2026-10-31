package forgekv.util;

import java.time.Instant;

/**
 * Minimal leveled logging to stderr. The level comes from the
 * {@code FORGEKV_LOG} environment variable ({@code error}, {@code warn},
 * {@code info} (default), {@code debug}).
 */
public final class Log {
    public enum Level { ERROR, WARN, INFO, DEBUG }

    private static volatile Level level = parse(System.getenv("FORGEKV_LOG"));

    private Log() {}

    private static Level parse(String s) {
        if (s == null) {
            return Level.INFO;
        }
        return switch (s.trim().toLowerCase()) {
            case "error" -> Level.ERROR;
            case "warn", "warning" -> Level.WARN;
            case "debug", "trace" -> Level.DEBUG;
            default -> Level.INFO;
        };
    }

    public static void setLevel(Level l) {
        level = l;
    }

    public static boolean enabled(Level l) {
        return l.ordinal() <= level.ordinal();
    }

    public static void error(String msg) {
        log(Level.ERROR, msg);
    }

    public static void warn(String msg) {
        log(Level.WARN, msg);
    }

    public static void info(String msg) {
        log(Level.INFO, msg);
    }

    public static void debug(String msg) {
        log(Level.DEBUG, msg);
    }

    private static void log(Level l, String msg) {
        if (!enabled(l)) {
            return;
        }
        String line = Instant.now() + " " + String.format("%5s", l.name()) + " ["
                + Thread.currentThread().getName() + "] " + msg;
        System.err.println(line);
    }
}
