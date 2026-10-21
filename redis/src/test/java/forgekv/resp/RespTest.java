package forgekv.resp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import forgekv.util.ByteBuf;
import forgekv.util.Bytes;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class RespTest {
    private static ByteBuf buf(String s) {
        return buf(s.getBytes(StandardCharsets.ISO_8859_1));
    }

    private static ByteBuf buf(byte[] b) {
        ByteBuf out = new ByteBuf(16);
        out.put(b);
        return out;
    }

    private static Frame p(String input) throws ProtocolException {
        return Resp.parse(buf(input));
    }

    private static Frame bulk(String s) {
        return Frame.bulk(s);
    }

    @Test
    void parsesScalars() throws Exception {
        assertEquals(Frame.OK, p("+OK\r\n"));
        assertEquals(Frame.error("ERR x"), p("-ERR x\r\n"));
        assertEquals(Frame.integer(-42), p(":-42\r\n"));
        assertEquals(bulk("hello"), p("$5\r\nhello\r\n"));
        assertEquals(bulk(""), p("$0\r\n\r\n"));
        assertEquals(Frame.NULL, p("$-1\r\n"));
        assertEquals(Frame.NULL_ARRAY, p("*-1\r\n"));
    }

    @Test
    void parsesCommandArray() throws Exception {
        assertEquals(Frame.array(bulk("SET"), bulk("k"), bulk("v")), p("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"));
    }

    @Test
    void binarySafeBulk() throws Exception {
        assertEquals(bulk("a\r\nb"), p("$4\r\na\r\nb\r\n"));
    }

    @Test
    void incompleteInputIsNotConsumed() throws Exception {
        byte[] full = "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n".getBytes(StandardCharsets.US_ASCII);
        for (int cut = 1; cut < full.length; cut++) {
            ByteBuf b = new ByteBuf(16);
            b.put(full, 0, cut);
            assertNull(Resp.parse(b), "cut at " + cut);
            assertEquals(cut, b.readable(), "buffer must be untouched");
        }
    }

    @Test
    void pipelinedFrames() throws Exception {
        ByteBuf b = buf("+A\r\n+B\r\n:1\r\n");
        assertEquals(Frame.simple("A"), Resp.parse(b));
        assertEquals(Frame.simple("B"), Resp.parse(b));
        assertEquals(Frame.integer(1), Resp.parse(b));
        assertNull(Resp.parse(b));
        assertTrue(b.isEmpty());
    }

    @Test
    void inlineCommands() throws Exception {
        assertEquals(Frame.array(bulk("SET"), bulk("foo"), bulk("bar")), p("SET  foo   bar\r\n"));
        assertEquals(Frame.array(bulk("PING")), p("PING\n"));
        assertNull(p("PIN"));
    }

    @Test
    void protocolErrors() {
        assertThrows(ProtocolException.class, () -> p("$abc\r\n"));
        assertThrows(ProtocolException.class, () -> p("$-5\r\n"));
        assertThrows(ProtocolException.class, () -> p("$3\r\nabcXY"));
        assertThrows(ProtocolException.class, () -> p("*1\r\n!oops\r\n"));
    }

    @Test
    void roundtrip() throws Exception {
        Frame f = Frame.array(List.of(
                Frame.OK,
                Frame.error("ERR boom"),
                Frame.integer(Long.MIN_VALUE),
                Frame.bulk(Bytes.wrap(new byte[] {'b', 'i', 'n', 0, 'a', 'r', 'y'})),
                Frame.NULL,
                Frame.NULL_ARRAY,
                Frame.EMPTY_ARRAY));
        ByteBuf b = buf(f.toBytes().array());
        assertEquals(f, Resp.parse(b));
        assertTrue(b.isEmpty());
    }
}
