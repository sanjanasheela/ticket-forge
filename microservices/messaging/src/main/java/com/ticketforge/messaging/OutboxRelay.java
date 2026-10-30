package com.ticketforge.messaging;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Publishes unsent outbox rows to Kafka, oldest first. Delivery is at least once: if the process
 * dies after Kafka accepted a record but before the row is marked sent, the record is sent again,
 * which is why every consumer is idempotent ({@link ProcessedEvents}).
 *
 * <p>Rows are claimed with {@code FOR UPDATE SKIP LOCKED}, so running several instances of a
 * service is safe. The Kafka send is waited on inside the transaction on purpose: a failed send
 * rolls back, leaving the row unsent for the next tick.
 */
@Component
class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH = 200;

    private record Row(long id, String topic, String key, String type, String payload) {
    }

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final KafkaTemplate<String, String> kafka;
    private final MeterRegistry meters;

    OutboxRelay(JdbcClient jdbc, TransactionTemplate tx, KafkaTemplate<String, String> kafka, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.kafka = kafka;
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
            // Kafka down or slow: rows stay unsent and are retried on the next tick. Never crash the scheduler.
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
        for (Row row : rows) {
            ProducerRecord<String, String> record = new ProducerRecord<>(row.topic(), row.key(), row.payload());
            record.headers().add(new RecordHeader("type", row.type().getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("eventId", Long.toString(row.id()).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            try {
                kafka.send(record).get(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } catch (Exception e) {
                throw new IllegalStateException("Kafka send failed for outbox row " + row.id(), e);
            }
            jdbc.sql("UPDATE outbox SET sent_at = now() WHERE id = :id").param("id", row.id()).update();
        }
        if (!rows.isEmpty()) {
            meters.counter("ticketforge.outbox.published").increment(rows.size());
        }
        return rows.size();
    }
}
