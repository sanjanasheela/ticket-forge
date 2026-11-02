package com.ticketforge.catalog;

import java.time.Duration;
import java.time.Instant;

/** Everything the booking path needs to know about a show, read once per request. */
public record ShowRules(
        long showId,
        long venueId,
        ShowCategory category,
        Instant startsAt,
        Instant saleOpensAt,
        Duration holdTtl,
        boolean queueEnabled,
        int maxSeatsPerHold,
        Integer maxHoldsPerUser,
        int admitRatePerSec,
        int gaShards,
        String currency,
        boolean active) {

    public boolean saleOpen(Instant now) {
        return active && !now.isBefore(saleOpensAt) && now.isBefore(startsAt);
    }
}
