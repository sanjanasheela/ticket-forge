package com.ticketforge.contracts;

import java.time.Instant;
import java.util.List;

/**
 * Everything reservation-service needs to know about a show, sent on every create and price change.
 * It is a full snapshot, not a delta, so consumers simply upsert it: applying the same snapshot
 * twice, or out of order with an older one for the same show, converges to the latest state.
 */
public record ShowSnapshot(
        long showId,
        long venueId,
        String category,          // MOVIE or CONCERT
        Instant startsAt,
        Instant saleOpensAt,
        int holdTtlSec,
        boolean queueEnabled,
        int maxSeatsPerHold,
        Integer maxHoldsPerUser,  // null = unlimited
        int admitRatePerSec,
        int gaShards,
        String currency,
        String status,            // ACTIVE or CANCELLED
        List<TierPrice> prices) {

    public record TierPrice(long tierId, long priceMinor) {
    }
}
