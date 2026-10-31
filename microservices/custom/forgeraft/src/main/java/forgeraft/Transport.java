package forgeraft;

import java.io.IOException;

public interface Transport {
    /** Blocking RPC. Throws IOException when the peer is unreachable or partitioned. */
    Msg call(String from, String to, Msg m) throws IOException;
}
