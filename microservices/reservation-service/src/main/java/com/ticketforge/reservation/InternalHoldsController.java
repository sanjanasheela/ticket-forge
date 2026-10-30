package com.ticketforge.reservation;

import java.time.Instant;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ticketforge.contracts.SagaMessages.HoldInfo;
import com.ticketforge.shared.ApiException;

/**
 * Step 1 of the checkout saga: booking-service asks whether this user's hold is still live and what it costs.
 * Guarded by the internal token; never routed through Kong.
 */
@RestController
class InternalHoldsController {

    private final ReservationService reservations;
    private final java.time.Clock clock;

    InternalHoldsController(ReservationService reservations, java.time.Clock clock) {
        this.reservations = reservations;
        this.clock = clock;
    }

    @GetMapping("/internal/holds/{holdId}")
    HoldInfo hold(@PathVariable UUID holdId, @RequestParam UUID userId) {
        Instant now = clock.instant();
        return reservations.findActive(holdId, userId, now)
                .map(h -> new HoldInfo(h.holdId(), h.showId(), h.totalMinor(), h.currency(), h.expiresAt(),
                        reservations.items(holdId).stream()
                                .map(i -> new HoldInfo.Item(i.seatId(), i.tierId(), i.label(), i.quantity(), i.priceMinor())).toList()))
                .orElseThrow(() -> reservations.exists(holdId, userId)
                        ? new ApiException(HttpStatus.CONFLICT, "HOLD_EXPIRED", "This hold has expired or was released")
                        : ApiException.notFound("HOLD_NOT_FOUND", "Hold not found"));
    }
}
