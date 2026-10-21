package forgekv.server;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class GlobTest {
    private static boolean m(String pattern, String text) {
        return Glob.match(pattern.getBytes(StandardCharsets.UTF_8), text.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void basics() {
        assertTrue(m("*", ""));
        assertTrue(m("*", "anything"));
        assertTrue(m("user:*", "user:42"));
        assertFalse(m("user:*", "usr:42"));
        assertTrue(m("h?llo", "hello"));
        assertFalse(m("h?llo", "hllo"));
        assertTrue(m("*:42:*", "a:42:b"));
        assertTrue(m("a*b*c", "aXXbYYc"));
        assertFalse(m("a*b*c", "aXXbYY"));
    }

    @Test
    void classesAndEscapes() {
        assertTrue(m("h[ae]llo", "hallo"));
        assertTrue(m("h[ae]llo", "hello"));
        assertFalse(m("h[ae]llo", "hillo"));
        assertTrue(m("h[^e]llo", "hallo"));
        assertFalse(m("h[^e]llo", "hello"));
        assertTrue(m("h[a-b]llo", "hbllo"));
        assertTrue(m("a\\*b", "a*b"));
        assertFalse(m("a\\*b", "aXb"));
    }

    @Test
    void noExponentialBlowup() {
        byte[] text = new byte[5_000];
        Arrays.fill(text, (byte) 'a');
        assertFalse(Glob.match("a*a*a*a*a*a*a*a*b".getBytes(StandardCharsets.US_ASCII), text));
    }
}
