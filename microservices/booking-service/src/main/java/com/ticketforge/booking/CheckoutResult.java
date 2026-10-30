package com.ticketforge.booking;

import java.util.UUID;

/** What a checkout attempt came to. The same four outcomes (and HTTP statuses) as the Phase 1 monolith. */
record CheckoutResult(Outcome outcome, UUID bookingId, UUID paymentId, String message) {

    enum Outcome { CONFIRMED, PAYMENT_PENDING, PAYMENT_FAILED, HOLD_EXPIRED_REFUNDED }
}
