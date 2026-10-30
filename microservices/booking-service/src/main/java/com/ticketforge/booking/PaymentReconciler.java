package com.ticketforge.booking;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.ticketforge.booking.Saga.State;
import com.ticketforge.payment.PaymentService;

/**
 * Resolves payments stuck in PENDING because the gateway timed out: ask the gateway what it recorded
 * under our idempotency key, then either carry the saga on (money was taken) or fail it (it was not).
 */
@Component
class PaymentReconciler {

    private static final Logger log = LoggerFactory.getLogger(PaymentReconciler.class);

    private final PaymentService payments;
    private final SagaRepository sagas;
    private final CheckoutSaga saga;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final Duration after;

    PaymentReconciler(PaymentService payments, SagaRepository sagas, CheckoutSaga saga, TransactionTemplate tx, Clock clock,
            @Value("${ticketforge.payment.reconcile-after}") Duration after) {
        this.payments = payments;
        this.sagas = sagas;
        this.saga = saga;
        this.tx = tx;
        this.clock = clock;
        this.after = after;
    }

    @Scheduled(fixedDelay = 5000)
    void reconcile() {
        try {
            for (var p : payments.pendingOlderThan(clock.instant().minus(after), 200)) {
                var owner = sagas.findByHold(p.holdId());
                payments.gateway().lookup(p.id().toString()).ifPresentOrElse(result -> {
                    if (result.succeeded()) {
                        owner.ifPresent(s -> {
                            saga.moneyTaken(s.id(), p.id(), p.holdId(), p.userId(), result.gatewayRef(), false);
                            log.info("Reconciled payment_id={} outcome=charged", p.id());
                        });
                    } else {
                        fail(p.id(), owner.map(Saga::id).orElse(null), result.declineReason());
                    }
                }, () -> fail(p.id(), owner.map(Saga::id).orElse(null), "gateway_has_no_record"));
            }
        } catch (RuntimeException e) {
            log.warn("Reconciliation failed, will retry: {}", e.toString());
        }
    }

    private void fail(java.util.UUID paymentId, java.util.UUID sagaId, String reason) {
        tx.executeWithoutResult(status -> {
            payments.markFailed(paymentId, reason);
            if (sagaId != null) {
                sagas.move(sagaId, List.of(State.CHARGING, State.CHARGE_UNKNOWN), State.FAILED, null, reason);
            }
        });
    }
}
