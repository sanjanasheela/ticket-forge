package com.ticketforge.inventory;

import java.util.List;

/** Published inside the transaction that changed the seats; listeners act after commit. */
public record SeatsChanged(long showId, Change change, List<Long> seatIds) {

    public enum Change { HELD, RELEASED, BOOKED }
}
