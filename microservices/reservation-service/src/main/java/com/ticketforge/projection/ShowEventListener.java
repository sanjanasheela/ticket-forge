package com.ticketforge.projection;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ticketforge.contracts.ShowSnapshot;
import com.ticketforge.contracts.Topics;
import com.ticketforge.messaging.MessageBus;

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
    private final MessageBus bus;

    ShowEventListener(ShowProjection projection, VenueLayoutClient layouts, JsonMapper json, MessageBus bus) {
        this.projection = projection;
        this.layouts = layouts;
        this.json = json;
        this.bus = bus;
    }

    @PostConstruct
    void subscribe() {
        bus.subscribe("reservation-service", Topics.CATALOG_SHOWS, this::onShow);
    }

    void onShow(String payload) {
        ShowSnapshot snapshot = json.readValue(payload, ShowSnapshot.class);
        projection.upsertShow(snapshot);
        if (!projection.inventoryReady(snapshot.showId())) {
            projection.materialiseInventory(snapshot, layouts.fetch(snapshot.venueId()));
        }
        log.debug("Applied ShowPublished show_id={}", snapshot.showId());
    }
}
