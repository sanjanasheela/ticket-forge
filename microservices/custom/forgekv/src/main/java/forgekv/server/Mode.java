package forgekv.server;

import forgekv.storage.Clock;

/** Who is issuing a command. */
public enum Mode {
    /** A regular client: real clock, writes refused on replicas. */
    CLIENT,
    /**
     * The replication stream from our leader: frozen clock (the stream is
     * time-independent, like the AOF) and writes always allowed.
     */
    REPLICATED;

    public long now() {
        return this == CLIENT ? Clock.nowMs() : 0;
    }
}
