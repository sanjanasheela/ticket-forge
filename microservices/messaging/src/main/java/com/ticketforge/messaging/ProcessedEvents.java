package com.ticketforge.messaging;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Idempotent consumer. Call {@link #firstTime} inside the same transaction that applies the event:
 * if the transaction rolls back the mark disappears with it, so the event is retried; if the event
 * was already applied, the unique key says so and the handler skips it.
 */
@Component
public class ProcessedEvents {

    private final JdbcClient jdbc;

    ProcessedEvents(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** @return true if this is the first time {@code consumer} sees {@code eventId} */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean firstTime(String consumer, String eventId) {
        return jdbc.sql("INSERT INTO processed_events (consumer, event_id) VALUES (:c, :e) ON CONFLICT DO NOTHING")
                .param("c", consumer).param("e", eventId).update() == 1;
    }
}
