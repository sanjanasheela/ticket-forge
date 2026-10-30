package com.ticketforge.inventory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import com.ticketforge.shared.Sql;

/**
 * The availability check *is* the WHERE clause: one round trip, row locks held only for the
 * statement's rows. Concurrent claimers of the same seat queue on its row lock, then re-check the
 * WHERE clause against the committed winner and match nothing.
 */
@Component
class AtomicConditionalStrategy implements SeatClaimStrategy {

    private final JdbcClient jdbc;

    AtomicConditionalStrategy(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public LockStrategy kind() {
        return LockStrategy.ATOMIC_CONDITIONAL;
    }

    @Override
    public List<ClaimedSeat> claimSeats(long showId, List<Long> seatIds, UUID holdId, Instant heldUntil, Instant now) {
        List<ClaimedSeat> claimed = jdbc.sql("""
                        UPDATE show_seats
                        SET status = 'HELD', hold_id = :h, held_until = :until, version = version + 1
                        WHERE show_id = :s AND venue_seat_id IN (:ids)
                          AND (status = 'AVAILABLE' OR (status = 'HELD' AND held_until < :now))
                        RETURNING venue_seat_id, tier_id, section, row_label, seat_number""")
                .param("h", holdId)
                .param("until", Sql.ts(heldUntil))
                .param("s", showId)
                .param("ids", seatIds)
                .param("now", Sql.ts(now))
                .query((rs, i) -> new ClaimedSeat(rs.getLong(1), rs.getLong(2),
                        rs.getString(3) + " " + rs.getString(4) + "-" + rs.getInt(5)))
                .list();
        if (claimed.size() != seatIds.size()) {
            Set<Long> got = new HashSet<>();
            claimed.forEach(c -> got.add(c.seatId()));
            List<Long> missing = new ArrayList<>(seatIds);
            missing.removeAll(got);
            throw SeatClaimStrategy.seatsUnavailable(missing);   // rolls back the seats we did get
        }
        return claimed;
    }

    @Override
    public void claimGa(long showId, long tierId, int quantity) {
        int updated = jdbc.sql("""
                        UPDATE ga_inventory SET held = held + :q, version = version + 1
                        WHERE show_id = :s AND tier_id = :t AND capacity - held - sold >= :q""")
                .param("q", quantity).param("s", showId).param("t", tierId)
                .update();
        if (updated == 0) {
            boolean exists = jdbc.sql("SELECT 1 FROM ga_inventory WHERE show_id = :s AND tier_id = :t")
                    .param("s", showId).param("t", tierId).query(Integer.class).optional().isPresent();
            throw exists ? SeatClaimStrategy.gaSoldOut(tierId) : SeatClaimStrategy.notGaTier(tierId);
        }
    }
}
