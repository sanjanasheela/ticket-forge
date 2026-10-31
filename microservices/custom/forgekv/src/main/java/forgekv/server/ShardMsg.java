package forgekv.server;

import forgekv.resp.Frame;
import forgekv.util.Bytes;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.function.Consumer;

/** Work posted to a shard's mailbox. */
public sealed interface ShardMsg {
    /**
     * Execute commands in order; once they are durable, {@code reply} runs on
     * the {@code origin} shard's thread with one frame per command.
     */
    record Exec(List<Bytes[]> cmds, Mode mode, Shard origin, Consumer<Frame[]> reply) implements ShardMsg {}

    /** A freshly accepted client connection to serve. */
    record Conn(SocketChannel channel) implements ShardMsg {}

    /** Run arbitrary code on the shard's thread (stats, rewrites, replies, wake-ups...). */
    record Run(Consumer<Shard> task) implements ShardMsg {}

    record Shutdown() implements ShardMsg {}
}
