package com.ticketforge.shared;

import java.sql.Timestamp;
import java.time.Instant;

/** pgjdbc cannot bind {@link Instant} directly; timestamps go through these helpers. */
public final class Sql {

    private Sql() {
    }

    public static Timestamp ts(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    public static Instant instant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
