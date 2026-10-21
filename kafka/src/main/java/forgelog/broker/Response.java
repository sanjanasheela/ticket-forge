package forgelog.broker;

import forgelog.log.Region;
import forgelog.protocol.Wire.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A response frame under construction: encoded fields interleaved with file
 * {@link Region}s that are sent with {@code transferTo} (zero-copy).
 */
final class Response {
    private final List<Object> parts = new ArrayList<>();
    private Writer cur = new Writer(256);
    private long total;

    Response(int correlationId) {
        cur.i32(0).i32(correlationId); // size is patched in finish()
    }

    Writer w() {
        return cur;
    }

    void region(Region r) {
        flushWriter();
        parts.add(r);
        total += r.length();
    }

    private void flushWriter() {
        ByteBuffer b = cur.toBuffer();
        if (b.hasRemaining()) {
            parts.add(b);
            total += b.remaining();
        }
        cur = new Writer(256);
    }

    /** The parts to write, in order: {@link ByteBuffer}s and {@link Region}s. */
    List<Object> finish() {
        flushWriter();
        ((ByteBuffer) parts.getFirst()).putInt(0, (int) (total - 4));
        return parts;
    }
}
