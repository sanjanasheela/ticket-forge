package com.ticketforge.booking;

import java.util.UUID;

/** One checkout, as stored in the {@code sagas} table. See V1__booking.sql for what each state means. */
record Saga(UUID id, UUID holdId, UUID userId, long showId, long totalMinor, String currency, String items,
        UUID paymentId, String gatewayRef, State state, UUID bookingId, String reason) {

    enum State { CHARGING, CHARGE_UNKNOWN, CHARGED, BOOKED, REFUNDING, REFUNDED, FAILED }
}
