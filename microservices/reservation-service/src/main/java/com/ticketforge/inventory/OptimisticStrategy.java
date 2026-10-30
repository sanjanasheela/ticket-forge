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
 * Read without locks, then write only if the version is unchanged. A changed version means
 * someone else got there first: throw {@link RetryableClaimException} and let the caller retry the
 * whole transaction (the retry usually turns into a clean "seat taken"). Cheap when contention is
 * low; under a stampede most work is wasted on retries.
 */
@Component
class OptimisticStrategy implements SeatClaimStrategy {

    private final JdbcClient jdbc;
    private final SeatRows rows;

    OptimisticStrategy(JdbcClient jdbc, SeatRows rows) {
        this.jdbc = jdbc;
        this.rows = rows;
    }

    @Override
    public LockStrategy kind() {
        return LockStrategy.OPTIMISTIC;
    }

    @Override
    public List<ClaimedSeat> claimSeats(long showId, List<Long> seatIds, UUID holdId, Instant heldUntil, Instant now) {
        List<RowSeat> seen = rows.seats(showId, seatIds, false);
        List<Long> unavailable = new ArrayList<>(seatIds);
        seen.stream().filter(s -> !s.occupied(now)).forEach(s -> unavailable.remove(s.seatId()));
        if (!unavailable.isEmpty()) {
            throw SeatClaimStrategy.seatsUnavailable(unavailable);
        }
        for (RowSeat s : seen) {
            int updated = jdbc.sql("""
                            UPDATE show_seats SET status = 'HELD', hold_id = :h, held_until = :until, version = version + 1
                            WHERE show_id = :s AND venue_seat_id = :id AND version = :v""")
                    .param("h", holdId).param("until", Sql.ts(heldUntil)).param("s", showId)
                    .param("id", s.seatId()).param("v", s.version())
                    .update();
            if (updated == 0) {
                throw new RetryableClaimException("Seat " + s.seatId() + " changed since it was read");
            }
        }
        return seen.stream().map(s -> new ClaimedSeat(s.seatId(), s.tierId(), s.label())).toList();
    }

    @Override
    public void claimGa(long showId, long tierId, int quantity) {
        Map<String, Object> row = jdbc.sql("SELECT capacity, held, sold, version FROM ga_inventory WHERE show_id = :s AND tier_id = :t")
                .param("s", showId).param("t", tierId)
                .query().listOfRows().stream().findFirst()
                .orElseThrow(() -> SeatClaimStrategy.notGaTier(tierId));
        int available = (Integer) row.get("capacity") - (Integer) row.get("held") - (Integer) row.get("sold");
        if (available < quantity) {
            throw SeatClaimStrategy.gaSoldOut(tierId);
        }
        int updated = jdbc.sql("""
                        UPDATE ga_inventory SET held = held + :q, version = version + 1
                        WHERE show_id = :s AND tier_id = :t AND version = :v""")
                .param("q", quantity).param("s", showId).param("t", tierId).param("v", row.get("version"))
                .update();
        if (updated == 0) {
            throw new RetryableClaimException("GA tier " + tierId + " changed since it was read");
        }
    }
}
