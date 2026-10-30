package com.ticketforge.inventory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import com.ticketforge.shared.Sql;

/**
 * Classic SELECT ... FOR UPDATE in seat-id order, check in Java, then UPDATE. Correct and easy to
 * reason about; under contention every claimer waits for the lock holder's whole transaction.
 */
@Component
class PessimisticStrategy implements SeatClaimStrategy {

    private final JdbcClient jdbc;
    private final SeatRows rows;

    PessimisticStrategy(JdbcClient jdbc, SeatRows rows) {
        this.jdbc = jdbc;
        this.rows = rows;
    }

    @Override
    public LockStrategy kind() {
        return LockStrategy.PESSIMISTIC;
    }

    @Override
    public List<ClaimedSeat> claimSeats(long showId, List<Long> seatIds, UUID holdId, Instant heldUntil, Instant now) {
        List<RowSeat> locked = rows.seats(showId, seatIds, true);
        List<Long> unavailable = new ArrayList<>(seatIds);
        locked.stream().filter(s -> !s.occupied(now)).forEach(s -> unavailable.remove(s.seatId()));
        if (!unavailable.isEmpty()) {
            throw SeatClaimStrategy.seatsUnavailable(unavailable);
        }
        jdbc.sql("""
                        UPDATE show_seats SET status = 'HELD', hold_id = :h, held_until = :until, version = version + 1
                        WHERE show_id = :s AND venue_seat_id IN (:ids)""")
                .param("h", holdId).param("until", Sql.ts(heldUntil)).param("s", showId).param("ids", seatIds)
                .update();
        return locked.stream().map(s -> new ClaimedSeat(s.seatId(), s.tierId(), s.label())).toList();
    }

    @Override
    public void claimGa(long showId, long tierId, int quantity) {
        Map<String, Object> row = jdbc.sql("""
                        SELECT capacity, held, sold FROM ga_inventory WHERE show_id = :s AND tier_id = :t FOR UPDATE""")
                .param("s", showId).param("t", tierId)
                .query().listOfRows().stream().findFirst()
                .orElseThrow(() -> SeatClaimStrategy.notGaTier(tierId));
        int available = (Integer) row.get("capacity") - (Integer) row.get("held") - (Integer) row.get("sold");
        if (available < quantity) {
            throw SeatClaimStrategy.gaSoldOut(tierId);
        }
        jdbc.sql("UPDATE ga_inventory SET held = held + :q, version = version + 1 WHERE show_id = :s AND tier_id = :t")
                .param("q", quantity).param("s", showId).param("t", tierId)
                .update();
    }
}
