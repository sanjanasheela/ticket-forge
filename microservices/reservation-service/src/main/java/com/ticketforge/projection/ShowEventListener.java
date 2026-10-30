package com.ticketforge.projection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import com.ticketforge.contracts.ShowSnapshot;
import com.ticketforge.contracts.Topics;

import tools.jackson.databind.json.JsonMapper;

/**
 * Consumes {@code catalog.shows}. If anything fails (catalog-service down, database busy) the record is
 * retried forever, one second apart, and the partition waits: a show must never be silently dropped,
 * because a lost event means a show that can never be booked.
 */
@Component
class ShowEventListener {

    private static final Logger log = LoggerFactory.getLogger(ShowEventListener.class);

    private final ShowProjection projection;
    private final VenueLayoutClient layouts;
    private final JsonMapper json;

    ShowEventListener(ShowProjection projection, VenueLayoutClient layouts, JsonMapper json) {
        this.projection = projection;
        this.layouts = layouts;
        this.json = json;
    }

    @KafkaListener(topics = Topics.CATALOG_SHOWS, groupId = "reservation-service")
    void onShow(String payload) {
        ShowSnapshot snapshot = json.readValue(payload, ShowSnapshot.class);
        projection.upsertShow(snapshot);
        if (!projection.inventoryReady(snapshot.showId())) {
            projection.materialiseInventory(snapshot, layouts.fetch(snapshot.venueId()));
        }
        log.debug("Applied ShowPublished show_id={}", snapshot.showId());
    }

    /** Boot attaches this to the listener container. Default would drop the record after 9 attempts. */
    @Bean
    CommonErrorHandler showEventErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1_000, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
