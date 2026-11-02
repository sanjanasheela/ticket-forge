package com.ticketforge.booking;

import java.time.Clock;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.ticketforge.payment.PaymentService;
import com.ticketforge.payment.PaymentService.PendingPayment;

/**
 * Resolves payments stuck in PENDING because the gateway timed out: ask the gateway what it
 * recorded under our idempotency key and finish the checkout (or fail it) accordingly.
 */
@Component
class PaymentReconciler {

    private static final Logger log = LoggerFactory.getLogger(PaymentReconciler.class);

    private final PaymentService payments;
    private final CheckoutService checkout;
    private final Clock clock;
    private final Duration after;

    PaymentReconciler(PaymentService payments, CheckoutService checkout, Clock clock,
            @Value("${ticketforge.payment.reconcile-after}") Duration after) {
        this.payments = payments;
        this.checkout = checkout;
        this.clock = clock;
        this.after = after;
    }

    @Scheduled(fixedDelay = 5000)
    void reconcile() {
        try {
            for (PendingPayment p : payments.pendingOlderThan(clock.instant().minus(after), 200)) {
                payments.gateway().lookup(p.id().toString()).ifPresentOrElse(result -> {
                    if (result.succeeded()) {
                        var r = checkout.finalizePaid(p.id(), p.holdId(), p.userId(), result.gatewayRef());
                        log.info("Reconciled payment_id={} outcome={}", p.id(), r.outcome());
                    } else {
                        payments.markFailed(p.id(), result.declineReason());
                    }
                }, () -> payments.markFailed(p.id(), "gateway_has_no_record"));
            }
        } catch (RuntimeException e) {
            log.warn("Reconciliation failed, will retry: {}", e.toString());
        }
    }
}
