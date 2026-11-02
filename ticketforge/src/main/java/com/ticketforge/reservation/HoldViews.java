package com.ticketforge.reservation;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public final class HoldViews {

    private HoldViews() {
    }

    /**
     * Exactly one form: {@code seatIds} (pick seats), {@code tierId + quantity} (GA), or
     * {@code tierId + bestAvailable} (any N reserved seats in a tier).
     */
    public record HoldRequest(@Size(max = 50) List<Long> seatIds, Long tierId, @Positive Integer quantity,
            @Positive Integer bestAvailable) {
    }

    public record HoldItem(Long seatId, long tierId, String label, int quantity, long priceMinor) {
    }

    public record HoldView(UUID holdId, long showId, String status, Instant expiresAt, long totalMinor, String currency,
            List<HoldItem> items) {
    }

    /** What checkout needs to know about a hold it is about to charge for. */
    public record ActiveHold(UUID holdId, long showId, long totalMinor, String currency, Instant expiresAt) {
    }
}
