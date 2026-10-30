package com.ticketforge.contracts;

/** Kafka topic names and event type names, in one place so producers and consumers cannot drift apart. */
public final class Topics {

    private Topics() {
    }

    /** catalog-service -> reservation-service. Key: showId. */
    public static final String CATALOG_SHOWS = "catalog.shows";
    /** Event type: a show was created or its prices changed. Payload: {@link ShowSnapshot}. */
    public static final String SHOW_PUBLISHED = "ShowPublished";

    // ---- checkout saga: booking-service (orchestrator) <-> reservation-service (participant) ----------------

    /** booking -> reservation. Key: holdId. Payload: {@link SagaMessages.ConfirmHold}. */
    public static final String SAGA_RESERVATION_COMMANDS = "saga.reservation.commands";
    public static final String CONFIRM_HOLD = "ConfirmHold";

    /** reservation -> booking. Key: holdId. Payload: {@link SagaMessages.ConfirmHoldReply}. */
    public static final String SAGA_RESERVATION_REPLIES = "saga.reservation.replies";
    public static final String CONFIRM_HOLD_REPLY = "ConfirmHoldReply";

    /** booking -> anyone interested (notifications, analytics). Key: bookingId. */
    public static final String BOOKING_EVENTS = "booking.events";
    public static final String BOOKING_CONFIRMED = "BookingConfirmed";
    public static final String BOOKING_REFUNDED = "BookingRefunded";
}
