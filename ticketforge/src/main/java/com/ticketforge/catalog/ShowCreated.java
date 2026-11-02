package com.ticketforge.catalog;

/**
 * Published inside the show-creation transaction. The inventory module listens synchronously and
 * materialises the show's seats in the same transaction, so a show never exists without inventory.
 */
public record ShowCreated(long showId, long venueId, int gaShards) {
}
