package forgekv.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class PubSubTest {
    private static final AtomicLong IDS = new AtomicLong(1);
    /** Wake-ups are irrelevant here: tests poll the queues directly. */
    private static final Executor NO_WAKE = r -> {};

    private static Bytes b(String s) {
        return Bytes.of(s);
    }

    private static PubSub.Client client(PubSub reg) {
        return new PubSub.Client(reg, IDS.getAndIncrement(), NO_WAKE, () -> {});
    }

    @Test
    void publishFanoutAndCleanup() {
        PubSub shared = new PubSub();
        ByteBuf out = new ByteBuf();
        PubSub.Client a = client(shared);
        PubSub.Client c2 = client(shared);
        a.subscribe(List.of(b("news")), out);
        c2.subscribe(List.of(b("news")), out);
        c2.psubscribe(List.of(b("n*")), out);

        assertEquals(3, shared.publish(b("news"), b("hi")));
        assertEquals(0, shared.publish(b("other"), b("hi")));
        Bytes msg = a.poll();
        assertArrayEquals("*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n".getBytes(StandardCharsets.US_ASCII),
                msg.array());

        a.close();
        assertEquals(1, shared.numSubscribers(b("news")));
        c2.close();
        assertEquals(0, shared.numSubscribers(b("news")));
        assertEquals(0, shared.numPatterns());
        assertTrue(shared.activeChannels(null).isEmpty());
    }

    @Test
    void slowConsumerIsFlaggedAndRemoved() {
        PubSub shared = new PubSub();
        ByteBuf out = new ByteBuf();
        PubSub.Client slow = client(shared);
        slow.subscribe(List.of(b("ch")), out);
        for (int i = 0; i < PubSub.CLIENT_QUEUE; i++) {
            assertEquals(1, shared.publish(b("ch"), b("x")));
        }
        assertFalse(slow.overflowed());
        // Queue is full: this publish is not delivered and flags the client.
        assertEquals(0, shared.publish(b("ch"), b("x")));
        assertTrue(slow.overflowed());
        assertEquals(0, shared.numSubscribers(b("ch")));
    }

    @Test
    void unsubscribeCounts() {
        PubSub shared = new PubSub();
        ByteBuf out = new ByteBuf();
        PubSub.Client c = client(shared);
        c.subscribe(List.of(b("a"), b("b")), out);
        c.psubscribe(List.of(b("p*")), out);
        assertEquals(3, c.count());
        c.unsubscribe(List.of(b("a")), out);
        assertEquals(2, c.count());
        c.unsubscribe(List.of(), out); // all channels
        c.punsubscribe(List.of(), out); // all patterns
        assertEquals(0, c.count());
        assertEquals(0, shared.publish(b("b"), b("x")));
    }
}
