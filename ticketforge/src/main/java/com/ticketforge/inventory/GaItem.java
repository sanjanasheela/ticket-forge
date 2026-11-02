package com.ticketforge.inventory;

/** GA units held by a hold. {@code shard} is null when they came from the single counter row. */
public record GaItem(long tierId, Integer shard, int quantity) {
}
