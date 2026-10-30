package com.ticketforge.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.kafka.KafkaContainer;

import com.ticketforge.contracts.ShowSnapshot;
import com.ticketforge.contracts.Topics;

import tools.jackson.databind.json.JsonMapper;

/** The event contract with reservation-service: what is published, when, and that a rollback publishes nothing. */
class CatalogEventsTests extends CatalogTestSupport {

    // @ServiceConnection hands the broker address to Spring without setting a property, so ask the container.
    @Autowired
    KafkaContainer kafka;

    private final JsonMapper json = JsonMapper.builder().findAndAddModules().build();

    @Test
    void creatingAShowPublishesItsFullSnapshotToKafka() {
        Created c = createShowFixture("CONCERT");

        ShowSnapshot snapshot = awaitSnapshot(c.showId(), p -> true);

        assertThat(snapshot.venueId()).isEqualTo(c.venueId());
        assertThat(snapshot.category()).isEqualTo("CONCERT");
        assertThat(snapshot.holdTtlSec()).as("concert default").isEqualTo(180);
        assertThat(snapshot.queueEnabled()).isTrue();
        assertThat(snapshot.status()).isEqualTo("ACTIVE");
        assertThat(snapshot.prices()).containsExactly(new ShowSnapshot.TierPrice(c.tierId(), 25_000));
    }

    @Test
    void aPriceChangePublishesANewSnapshotForTheSameKey() {
        Created c = createShowFixture("MOVIE");
        awaitSnapshot(c.showId(), p -> true);

        assertThat(mvc.put().uri("/admin/shows/" + c.showId() + "/prices")
                .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + adminToken())
                .content("[{\"tierId\":" + c.tierId() + ",\"priceMinor\":12345}]").exchange()).hasStatus(204);

        ShowSnapshot latest = awaitSnapshot(c.showId(), p -> p.prices().get(0).priceMinor() == 12_345);
        assertThat(latest.prices()).containsExactly(new ShowSnapshot.TierPrice(c.tierId(), 12_345));
    }

    @Test
    void theRelayMarksRowsSentOnlyAfterKafkaAcceptedThem() {
        Created c = createShowFixture("MOVIE");
        awaitSnapshot(c.showId(), p -> true);

        // the relay marks a row sent right after Kafka acknowledged it; allow it a moment
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(jdbc.sql("SELECT count(*) FROM outbox WHERE msg_key = :k AND sent_at IS NULL")
                        .param("k", Long.toString(c.showId())).query(Long.class).single()).isZero());
    }

    @Test
    void aRejectedShowLeavesNothingInTheOutbox() {
        long before = jdbc.sql("SELECT count(*) FROM outbox").query(Long.class).single();
        Created c = createShowFixture("MOVIE");   // 1 event row, 1 show => 1 outbox row

        // a show that fails validation (unpriced tier) must roll back, including its outbox row
        assertThat(postJson("/admin/shows", adminToken(), """
                {"eventId":%d,"venueId":%d,"startsAt":"%s","prices":[]}"""
                .formatted(c.eventId(), c.venueId(), java.time.Instant.now().plusSeconds(86400)))).hasStatus(400);

        long after = jdbc.sql("SELECT count(*) FROM outbox").query(Long.class).single();
        assertThat(after - before).as("only the successful show was recorded").isEqualTo(1);
    }

    // ---- helpers --------------------------------------------------------------------------------

    /** Reads {@code catalog.shows} from the beginning until a snapshot for {@code showId} matching the filter shows up. */
    private ShowSnapshot awaitSnapshot(long showId, java.util.function.Predicate<ShowSnapshot> accept) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(Topics.CATALOG_SHOWS));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (!r.key().equals(Long.toString(showId))) {
                        continue;
                    }
                    String type = new String(r.headers().lastHeader("type").value(), StandardCharsets.UTF_8);
                    assertThat(type).isEqualTo(Topics.SHOW_PUBLISHED);
                    ShowSnapshot snapshot = json.readValue(r.value(), ShowSnapshot.class);
                    if (accept.test(snapshot)) {
                        return snapshot;
                    }
                }
            }
        }
        throw new AssertionError("No matching snapshot for show " + showId + " arrived on " + Topics.CATALOG_SHOWS);
    }
}
