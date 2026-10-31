package forgekv.server.repl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import forgekv.server.Outbox;
import forgekv.util.Bytes;
import org.junit.jupiter.api.Test;

class ReplicationUnitTest {
    private static Bytes b(String s) {
        return Bytes.of(s);
    }

    private static Outbox outbox() {
        return new Outbox(16, r -> {}, () -> {});
    }

    @Test
    void backlogWindow() {
        Backlog bl = new Backlog(8);
        bl.push("abcd".getBytes());
        assertEquals(b("abcd"), bl.since(0));
        assertEquals(Bytes.EMPTY, bl.since(4));
        assertNull(bl.since(5));
        bl.push("efghij".getBytes());
        assertEquals(2, bl.start());
        assertEquals(10, bl.end);
        assertNull(bl.since(1), "fell out of the backlog");
        assertEquals(b("ghij"), bl.since(6));
    }

    @Test
    void snapshotMergeDropsPreSnapshotWrites() {
        Replication repl = new Replication(2, 1 << 20);
        Outbox rx = outbox();
        assertEquals(Replication.Registered.FULL, repl.register(7, rx, "x", 1, "?", -1));
        repl.feed(0, b("s0-before|")); // shard 0 not snapshotted yet: dropped
        repl.snapshotPart(0, 7, b("S0|"));
        repl.feed(0, b("s0-after|")); // kept, after S0
        repl.feed(1, b("s1-before|")); // dropped
        repl.feed(-1, b("ctl|")); // control: dropped while syncing
        repl.snapshotPart(1, 7, b("S1|"));
        long end = repl.masterOffset();
        assertEquals(b("+FULLRESYNC " + repl.replid() + " " + end + "\r\n$15\r\n"), rx.poll());
        assertEquals(b("S0|s0-after|S1|"), rx.poll());
        // Online now: later writes stream straight through.
        repl.feed(1, b("live"));
        assertEquals(b("live"), rx.poll());
    }

    @Test
    void partialResyncFromBacklog() {
        Replication repl = new Replication(1, 1 << 20);
        repl.feed(0, b("0123456789"));
        String replid = repl.replid();
        Outbox rx = outbox();
        assertEquals(Replication.Registered.CONTINUE, repl.register(1, rx, "x", 1, replid, 4));
        assertEquals(b("+CONTINUE " + replid + "\r\n"), rx.poll());
        assertEquals(b("456789"), rx.poll());
        // Unknown history => full sync.
        assertEquals(Replication.Registered.FULL, repl.register(2, outbox(), "x", 1, "deadbeef", 4));
        String info = repl.info();
        assertTrue(info.contains("sync_partial_ok:1") && info.contains("sync_partial_err:1"));
    }
}
