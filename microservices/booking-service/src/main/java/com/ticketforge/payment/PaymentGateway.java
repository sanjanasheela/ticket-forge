package com.ticketforge.payment;

import java.util.Optional;

/**
 * A card processor. {@code idempotencyKey} is our payment id: charging twice with the same key
 * charges once, like Stripe or Razorpay.
 */
public interface PaymentGateway {

    /** @throws GatewayTimeoutException when the outcome is unknown (the charge may or may not have happened) */
    ChargeResult charge(String idempotencyKey, long amountMinor, String currency);

    /** What the processor recorded for this key, if anything. Used to resolve timeouts. */
    Optional<ChargeResult> lookup(String idempotencyKey);

    void refund(String gatewayRef);

    record ChargeResult(boolean succeeded, String gatewayRef, String declineReason) {
    }

    class GatewayTimeoutException extends RuntimeException {

        public GatewayTimeoutException(String message) {
            super(message, null, false, false);
        }
    }
}
