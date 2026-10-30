package com.ticketforge.contracts;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Messages of the checkout saga (see docs/PHASE2_DESIGN.md section 5). */
public final class SagaMessages {

    private SagaMessages() {
    }

    /**
     * Command: turn this live hold into a confirmed one and its seats into BOOKED. The money has
     * already been taken. Idempotent: confirming an already confirmed hold answers "confirmed" again.
     */
    public record ConfirmHold(UUID sagaId, UUID holdId, UUID userId) {
    }

    /** Reply to {@link ConfirmHold}. {@code reason} is set when {@code confirmed} is false (e.g. HOLD_EXPIRED). */
    public record ConfirmHoldReply(UUID sagaId, UUID holdId, boolean confirmed, String reason) {
    }

    /** Read model of a hold, returned by reservation-service's internal API for the saga's first step. */
    public record HoldInfo(UUID holdId, long showId, long totalMinor, String currency, Instant expiresAt, List<Item> items) {

        /** One line of what is being bought: a reserved seat (seatId set) or GA units. */
        public record Item(Long seatId, long tierId, String label, int quantity, long priceMinor) {
        }
    }

    /** Emitted on {@link Topics#BOOKING_EVENTS}. */
    public record BookingEvent(UUID bookingId, UUID holdId, UUID userId, long showId, long totalMinor, String currency) {
    }
}
