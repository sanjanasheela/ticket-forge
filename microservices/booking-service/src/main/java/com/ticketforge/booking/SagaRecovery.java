package com.ticketforge.booking;

import java.time.Clock;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.ticketforge.booking.Saga.State;

/**
 * Finishes sagas that stalled, so a crash or a lost message delays a checkout but never strands money:
 * <ul>
 *   <li>CHARGED for too long: the reply never came, so ask again (ConfirmHold is idempotent);</li>
 *   <li>REFUNDING for too long: the process died mid-refund, so refund again (also idempotent).</li>
 * </ul>
 */
@Component
class SagaRecovery {

    private static final Logger log = LoggerFactory.getLogger(SagaRecovery.class);

    private final SagaRepository sagas;
    private final CheckoutSaga saga;
    private final Clock clock;
    private final Duration confirmAfter;
    private final Duration refundAfter;

    SagaRecovery(SagaRepository sagas, CheckoutSaga saga, Clock clock,
            @Value("${ticketforge.saga.resend-confirm-after:15s}") Duration confirmAfter,
            @Value("${ticketforge.saga.retry-refund-after:10s}") Duration refundAfter) {
        this.sagas = sagas;
        this.saga = saga;
        this.clock = clock;
        this.confirmAfter = confirmAfter;
        this.refundAfter = refundAfter;
    }

    @Scheduled(fixedDelay = 5000)
    void recover() {
        try {
            sagas.stuck(State.CHARGED, clock.instant().minus(confirmAfter), 100).forEach(saga::resendConfirm);
            sagas.stuck(State.REFUNDING, clock.instant().minus(refundAfter), 100).forEach(saga::refund);
        } catch (RuntimeException e) {
            log.warn("Saga recovery failed, will retry: {}", e.toString());
        }
    }
}
