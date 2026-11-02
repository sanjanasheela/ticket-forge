package com.ticketforge.reservation;

/** Thrown by {@link ReservationService#confirm}: the hold expired or lost a seat; roll back and refund. */
public class HoldNoLongerValidException extends RuntimeException {

    public HoldNoLongerValidException(String message) {
        super(message, null, false, false);
    }
}
