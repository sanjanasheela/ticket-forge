package com.ticketforge.reservation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Active expiry. Reserved seats also expire lazily on the claim path, but GA units only come back
 * here, so the interval matters most for concert pits (spec §5.4).
 */
@Component
class HoldSweeper {

    private static final Logger log = LoggerFactory.getLogger(HoldSweeper.class);

    private final ReservationService reservations;
    private final int batchSize;

    HoldSweeper(ReservationService reservations, @Value("${ticketforge.reservation.sweep-batch-size}") int batchSize) {
        this.reservations = reservations;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${ticketforge.reservation.sweep-interval}")
    void sweep() {
        try {
            int n;
            do {
                n = reservations.expireBatch(batchSize);
            } while (n == batchSize);
        } catch (RuntimeException e) {
            log.warn("Hold sweep failed, will retry next tick: {}", e.toString());
        }
    }
}
