package com.ticketforge.inventory;

import java.time.Instant;

/** One seat as read for a claim: layout, state, and optimistic version. */
record RowSeat(long seatId, long tierId, String section, String row, int number, boolean aisleLeft, boolean aisleRight,
        String status, Instant heldUntil, long version) {

    /** Lazy expiry: a HELD seat whose hold ran out counts as free. */
    boolean occupied(Instant now) {
        return "BOOKED".equals(status) || ("HELD".equals(status) && !heldUntil.isBefore(now));
    }

    String label() {
        return section + " " + row + "-" + number;
    }
}
