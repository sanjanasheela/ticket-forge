package com.ticketforge.messaging;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import tools.jackson.databind.json.JsonMapper;

/**
 * Transactional outbox. Call {@link #add} inside the same transaction that changes your state; the
 * {@link OutboxRelay} publishes the row to Kafka afterwards. MANDATORY propagation makes it an
 * error to call this outside a transaction, which would silently defeat the pattern.
 */
@Component
public class Outbox {

    private final JdbcClient jdbc;
    private final JsonMapper json;

    Outbox(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    /** @param key Kafka partition key; use the aggregate id so its events are consumed in order */
    @Transactional(propagation = Propagation.MANDATORY)
    public void add(String topic, String key, String type, Object payload) {
        jdbc.sql("INSERT INTO outbox (topic, msg_key, type, payload) VALUES (:t, :k, :ty, :p)")
                .param("t", topic).param("k", key).param("ty", type).param("p", json.writeValueAsString(payload))
                .update();
    }
}
