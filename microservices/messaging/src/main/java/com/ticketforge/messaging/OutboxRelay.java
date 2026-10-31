package com.ticketforge.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Publishes unsent outbox rows to the message bus (Kafka or forgelog), oldest first. Delivery is at least once: if the process
 * dies after Kafka accepted a record but before the row is marked sent, the record is sent again,
 * which is why every consumer is idempotent ({@link ProcessedEvents}).
 *
 * <p>Rows are claimed with {@code FOR UPDATE SKIP LOCKED}, so running several instances of a
 * service is safe. The batch's Kafka sends are waited on inside the transaction on purpose: a failed
 * send rolls back, leaving the rows unsent for the next tick.
 */
@Component
class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH = 200;

    private record Row(long id, String topic, String key, String type, String payload) {
    }

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final MessageBus bus;
    private final MeterRegistry meters;

    OutboxRelay(JdbcClient jdbc, TransactionTemplate tx, MessageBus bus, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.bus = bus;
        this.meters = meters;
    }

    @Scheduled(fixedDelayString = "${ticketforge.outbox.poll-interval:100ms}")
    void drain() {
        try {
            int sent;
            do {
                Integer n = tx.execute(status -> publishBatch());
                sent = n == null ? 0 : n;
            } while (sent == BATCH);
        } catch (RuntimeException e) {
            // Broker down or slow: rows stay unsent and are retried on the next tick. Never crash the scheduler.
            log.warn("Outbox relay will retry: {}", e.toString());
            meters.counter("ticketforge.outbox.relay.failures").increment();
        }
    }

    private int publishBatch() {
        List<Row> rows = jdbc.sql("""
                        SELECT id, topic, msg_key, type, payload FROM outbox
                        WHERE sent_at IS NULL ORDER BY id LIMIT :n FOR UPDATE SKIP LOCKED""")
                .param("n", BATCH)
                .query((rs, i) -> new Row(rs.getLong("id"), rs.getString("topic"), rs.getString("msg_key"),
                        rs.getString("type"), rs.getString("payload")))
                .list();
        if (rows.isEmpty()) {
            return 0;
        }
        // Send the whole batch without waiting between records, then wait once. Waiting for each ack in turn
        // capped throughput at 1 / ack-latency (the first benchmark pass backed up thousands of checkouts behind
        // it). Order per key is kept: the producer is idempotent, so retries cannot reorder records.
        List<CompletableFuture<?>> sends = new ArrayList<>(rows.size());
        for (Row row : rows) {
            sends.add(bus.send(row.topic(), row.key(), row.type(), Long.toString(row.id()), row.payload()));
        }
        try {
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            // Nothing is marked sent, so the transaction ends and every row is retried. Records the broker did accept
            // are sent again: at-least-once, which the consumers are built for.
            throw new IllegalStateException("Bus send failed for an outbox batch of " + rows.size(), e);
        }
        jdbc.sql("UPDATE outbox SET sent_at = now() WHERE id = ANY (string_to_array(:ids, ',')::bigint[])")
                .param("ids", rows.stream().map(r -> Long.toString(r.id())).collect(Collectors.joining(",")))
                .update();
        meters.counter("ticketforge.outbox.published").increment(rows.size());
        return rows.size();
    }
}
