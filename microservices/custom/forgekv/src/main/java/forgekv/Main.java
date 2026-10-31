package forgekv;

import forgekv.server.Config;
import forgekv.server.Server;
import forgekv.server.aof.AofConfig;
import forgekv.server.aof.FsyncPolicy;
import forgekv.server.aof.IoBackend;
import forgekv.util.Log;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/** ForgeKV: a Redis-compatible in-memory data store. */
public final class Main {
    private static final String VERSION = "0.1.0";

    private static final String USAGE = """
            ForgeKV - a Redis-compatible in-memory data store

            Usage: forgekv [OPTIONS]

            Options:
                  --bind <BIND>                    Address to bind to [default: 127.0.0.1]
              -p, --port <PORT>                    TCP port (default 6380 so it can run next to a real Redis on 6379) [default: 6380]
                  --shards <SHARDS>                Number of shards, each owned by one thread. 0 = one per CPU [default: 1]
                  --appendonly                     Persist every write to an append-only file and replay it on startup
                  --appendfilename <FILE>          Base name of the append-only files [default: appendonly.aof]
                  --appendfsync <POLICY>           When to fsync the AOF: always, everysec, no [default: everysec]
                  --aof-backend <BACKEND>          AOF I/O backend: auto (io_uring if available), uring, std [default: auto]
                  --replicaof <HOST> <PORT>        Replicate from a leader
                  --repl-backlog-size <BYTES>      Bytes of replication stream kept for partial resyncs [default: 1048576]
              -h, --help                           Print help
              -V, --version                        Print version
            """;

    private Main() {}

    public static void main(String[] argv) throws Exception {
        String bind = "127.0.0.1";
        int port = 6380;
        int shards = 1;
        boolean appendonly = false;
        String appendfilename = "appendonly.aof";
        FsyncPolicy fsync = FsyncPolicy.EVERYSEC;
        IoBackend backend = IoBackend.AUTO;
        String replHost = null;
        int replPort = 0;
        int backlog = 1 << 20;

        try {
            for (int i = 0; i < argv.length; i++) {
                String a = argv[i];
                String inline = null;
                int eq = a.indexOf('=');
                if (a.startsWith("--") && eq > 0) {
                    inline = a.substring(eq + 1);
                    a = a.substring(0, eq);
                }
                switch (a) {
                    case "--bind" -> bind = inline != null ? inline : value(argv, ++i, a);
                    case "-p", "--port" -> port = parsePort(inline != null ? inline : value(argv, ++i, a));
                    case "--shards" -> shards = parseNonNegative(inline != null ? inline : value(argv, ++i, a), a);
                    case "--appendonly" -> appendonly = true;
                    case "--appendfilename" -> appendfilename = inline != null ? inline : value(argv, ++i, a);
                    case "--appendfsync" -> fsync = FsyncPolicy.parse(inline != null ? inline : value(argv, ++i, a));
                    case "--aof-backend" -> backend = IoBackend.parse(inline != null ? inline : value(argv, ++i, a));
                    case "--replicaof" -> {
                        replHost = value(argv, ++i, a);
                        replPort = parsePort(value(argv, ++i, a));
                    }
                    case "--repl-backlog-size" ->
                            backlog = parseNonNegative(inline != null ? inline : value(argv, ++i, a), a);
                    case "-h", "--help" -> {
                        System.out.print(USAGE);
                        return;
                    }
                    case "-V", "--version" -> {
                        System.out.println("forgekv " + VERSION);
                        return;
                    }
                    default -> throw new IllegalArgumentException("unexpected argument '" + argv[i] + "'");
                }
            }
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage() + "\n\nFor more information, try '--help'.");
            System.exit(2);
        }

        Config config = new Config(
                appendonly ? new AofConfig(Path.of(appendfilename), fsync, backend) : null,
                shards,
                replHost,
                replPort,
                backlog);

        Server server = Server.start(bind, port, config);
        Log.info("ForgeKV v" + VERSION + " listening on " + server.addr().getHostString() + ":" + server.port()
                + " with " + server.shards() + " shard(s)");
        if (config.aof() != null) {
            Log.info("AOF enabled: " + config.aof().path() + " (appendfsync " + fsync + ", backend " + backend + ")");
        }
        if (replHost != null) {
            Log.info("replica of " + replHost + ":" + replPort);
        }

        CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Log.info("received shutdown signal, shutting down");
            server.shutdown();
            done.countDown();
        }, "forgekv-shutdown"));
        done.await();
    }

    private static String value(String[] argv, int i, String flag) {
        if (i >= argv.length) {
            throw new IllegalArgumentException("a value is required for '" + flag + "'");
        }
        return argv[i];
    }

    private static int parsePort(String s) {
        try {
            int p = Integer.parseInt(s);
            if (p < 0 || p > 65535) {
                throw new NumberFormatException();
            }
            return p;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid port '" + s + "'");
        }
    }

    private static int parseNonNegative(String s, String flag) {
        try {
            int n = Integer.parseInt(s);
            if (n < 0) {
                throw new NumberFormatException();
            }
            return n;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid value '" + s + "' for '" + flag + "'");
        }
    }
}
