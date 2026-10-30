package com.ticketforge.inventory;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.http.HttpStatus;

import com.ticketforge.shared.ApiException;

/**
 * One way of taking seats and single-row GA units. Called inside the caller's transaction; a
 * failure throws (rolling everything back). Every implementation must pass the same conformance
 * suite: N concurrent claims on the same seats produce exactly one winner, GA never oversells.
 */
interface SeatClaimStrategy {

    LockStrategy kind();

    /** {@code seatIds} are sorted ascending. All-or-nothing. */
    List<ClaimedSeat> claimSeats(long showId, List<Long> seatIds, UUID holdId, Instant heldUntil, Instant now);

    /** Takes {@code quantity} units from the single counter row of a GA tier. */
    void claimGa(long showId, long tierId, int quantity);

    static ApiException seatsUnavailable(List<Long> seatIds) {
        return new ApiException(HttpStatus.CONFLICT, "SEATS_UNAVAILABLE",
                "Some of the selected seats were just taken", Map.of("seatIds", seatIds));
    }

    static ApiException gaSoldOut(long tierId) {
        return new ApiException(HttpStatus.CONFLICT, "GA_SOLD_OUT",
                "Not enough tickets left in this tier", Map.of("tierId", tierId));
    }

    static ApiException notGaTier(long tierId) {
        return ApiException.badRequest("NOT_A_GA_TIER", "Tier " + tierId + " is not a general-admission tier of this show");
    }
}
