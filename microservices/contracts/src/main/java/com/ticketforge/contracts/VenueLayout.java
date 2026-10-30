package com.ticketforge.contracts;

import java.util.List;

/**
 * A venue's seats and general-admission tiers, served by catalog-service so that reservation-service
 * can build a show's inventory without reading catalog tables. A 50,000-seat stadium is a few MB of JSON.
 */
public record VenueLayout(long venueId, List<Seat> seats, List<GaTier> gaTiers) {

    public record Seat(long id, long tierId, String section, String rowLabel, int rowOrder, int seatNumber,
            boolean aisleLeft, boolean aisleRight) {
    }

    public record GaTier(long tierId, int capacity) {
    }
}
