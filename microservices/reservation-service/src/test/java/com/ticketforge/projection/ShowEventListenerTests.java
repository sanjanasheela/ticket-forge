package com.ticketforge.projection;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import com.ticketforge.IntegrationTest;
import com.ticketforge.StubCatalog;
import com.ticketforge.contracts.ShowSnapshot;
import com.ticketforge.contracts.Topics;
import com.ticketforge.contracts.VenueLayout;

import tools.jackson.databind.json.JsonMapper;

/** catalog-service -> Kafka -> reservation-service, with a stub in place of catalog's layout endpoint. */
class ShowEventListenerTests extends IntegrationTest {

    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    StubCatalog catalog;
    @Autowired
    JsonMapper json;

    @AfterEach
    void catalogBackUp() {
        catalog.setDown(false);
    }

    private ShowSnapshot snapshot(long id, long price) {
        Instant now = Instant.now();
        return new ShowSnapshot(id, id, "CONCERT", now.plus(Duration.ofDays(1)), now.minusSeconds(60), 180, true, 6, 1, 200, 0,
                "INR", "ACTIVE", List.of(new ShowSnapshot.TierPrice(1, price)));
    }

    private VenueLayout venue(long id, int seats) {
        List<VenueLayout.Seat> list = new ArrayList<>();
        for (int i = 1; i <= seats; i++) {
            list.add(new VenueLayout.Seat(i, 1, "MAIN", "A", 0, i, false, false));
        }
        return new VenueLayout(id, list, List.of());
    }

    private void publish(ShowSnapshot s) {
        kafka.send(Topics.CATALOG_SHOWS, Long.toString(s.showId()), json.writeValueAsString(s));
    }

    private void awaitReady(long id) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(projection.inventoryReady(id)).isTrue());
    }

    @Test
    void aPublishedShowBecomesBookable() {
        long id = 8_800_001;
        catalog.register(venue(id, 20));

        publish(snapshot(id, 7000));

        awaitReady(id);
        assertThat(get("/shows/" + id + "/seats", null)).hasStatusOk().bodyJson().extractingPath("$.seats.length()").isEqualTo(20);
    }

    @Test
    void redeliveryOfTheSameEventChangesNothing() {
        long id = 8_800_002;
        catalog.register(venue(id, 10));
        publish(snapshot(id, 7000));
        awaitReady(id);

        publish(snapshot(id, 7000));      // Kafka delivers at least once
        publish(snapshot(id, 7000));
        publish(snapshot(id, 8000));      // a marker: when this price is visible, the duplicates before it have been processed
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(jdbc.sql("SELECT price_minor FROM show_tier_prices WHERE show_id = :s").param("s", id).query(Long.class).single()).isEqualTo(8000));

        assertThat(jdbc.sql("SELECT count(*) FROM show_seats WHERE show_id = :s").param("s", id).query(Integer.class).single()).isEqualTo(10);
        assertThat(catalog.callsFor(id)).as("inventory is built once; later events do not refetch the layout").isEqualTo(1);
    }

    @Test
    void anEventIsRetriedUntilCatalogComesBackNeverDropped() {
        long id = 8_800_003;
        catalog.register(venue(id, 6));
        catalog.setDown(true);

        publish(snapshot(id, 7000));

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(catalog.callsFor(id)).isGreaterThanOrEqualTo(2));
        assertThat(projection.inventoryReady(id)).as("still waiting for catalog").isFalse();

        catalog.setDown(false);

        awaitReady(id);
        assertThat(get("/shows/" + id + "/seats", null)).hasStatusOk();
    }
}
