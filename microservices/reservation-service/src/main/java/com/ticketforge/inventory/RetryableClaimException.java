package com.ticketforge.inventory;

/** An optimistic claim lost a race. The whole transaction should be retried. */
public class RetryableClaimException extends RuntimeException {

    public RetryableClaimException(String message) {
        super(message, null, false, false);
    }
}
