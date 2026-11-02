package com.ticketforge.inventory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.ticketforge.shared.ApiException;
import com.ticketforge.shared.Sql;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The inventory hot path. Every method must run inside the caller's transaction (MANDATORY), so
 * the hold row and the seats it claims commit or roll back together.
 */
@Service
@Transactional(propagation = Propagation.MANDATORY)
public class InventoryService {

    private final JdbcClient jdbc;
    private final SeatRows rows;
    private final InventorySettings settings;
    private final Map<LockStrategy, SeatClaimStrategy> strategies = new EnumMap<>(LockStrategy.class);
    private final ApplicationEventPublisher events;
    private final Counter gapRaces;

    InventoryService(JdbcClient jdbc, SeatRows rows, InventorySettings settings, List<SeatClaimStrategy> all,
            ApplicationEventPublisher events, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.rows = rows;
        this.settings = settings;
        all.forEach(s -> strategies.put(s.kind(), s));
        this.events = events;
        this.gapRaces = Counter.builder("ticketforge.gap.rule.races")
                .description("Orphan seats that appeared despite the gap check (concurrent claims on one row)")
                .register(meters);
    }

    @Transactional(propagation = Propagation.SUPPORTS)
    public LockStrategy strategy() {
        return settings.strategy();
    }

    /** Units still free in a GA tier (sharded or not). */
    @Transactional(propagation = Propagation.SUPPORTS)
    public int gaAvailable(long showId, long tierId) {
        return jdbc.sql("""
                        SELECT COALESCE(
                            (SELECT sum(capacity - held - sold) FROM ga_inventory_shards WHERE show_id = :s AND tier_id = :t),
                            (SELECT capacity - held - sold FROM ga_inventory WHERE show_id = :s AND tier_id = :t))""")
                .param("s", showId).param("t", tierId)
                .query(Integer.class).optional().orElse(0);
    }

    /** Claims exactly these reserved seats, all or nothing, enforcing the gap rule. */
    public List<ClaimedSeat> claimSeats(long showId, Collection<Long> seatIds, UUID holdId, Instant heldUntil, Instant now) {
        List<Long> sorted = new ArrayList<>(new TreeSet<>(seatIds));
        LockStrategy kind = settings.strategy();
        boolean gapRule = settings.gapRuleEnabled();

        List<RowSeat> before = null;
        if (gapRule) {
            // PESSIMISTIC locks the whole rows here, so the check below cannot be raced.
            before = rows.rowsOf(showId, sorted, kind == LockStrategy.PESSIMISTIC);
            checkGapRule(before, sorted, now);
        }

        List<ClaimedSeat> claimed = strategies.get(kind).claimSeats(showId, sorted, holdId, heldUntil, now);

        if (gapRule && kind != LockStrategy.PESSIMISTIC) {
            detectGapRace(showId, sorted, before, now);
        }
        events.publishEvent(new SeatsChanged(showId, SeatsChanged.Change.HELD, sorted));
        return claimed;
    }

    /**
     * "Any N seats in this tier": FOR UPDATE SKIP LOCKED, so concurrent buyers never wait on each
     * other and simply get different seats. The gap rule is not applied to automatic picks.
     */
    public List<ClaimedSeat> claimBestAvailable(long showId, long tierId, int count, UUID holdId, Instant heldUntil,
            Instant now) {
        List<ClaimedSeat> claimed = jdbc.sql("""
                        WITH picked AS (
                            SELECT venue_seat_id FROM show_seats
                            WHERE show_id = :s AND tier_id = :t
                              AND (status = 'AVAILABLE' OR (status = 'HELD' AND held_until < :now))
                            ORDER BY venue_seat_id
                            LIMIT :n
                            FOR UPDATE SKIP LOCKED)
                        UPDATE show_seats ss
                        SET status = 'HELD', hold_id = :h, held_until = :until, version = ss.version + 1
                        FROM picked
                        WHERE ss.show_id = :s AND ss.venue_seat_id = picked.venue_seat_id
                        RETURNING ss.venue_seat_id, ss.tier_id, ss.section, ss.row_label, ss.seat_number""")
                .param("s", showId).param("t", tierId).param("now", Sql.ts(now)).param("n", count)
                .param("h", holdId).param("until", Sql.ts(heldUntil))
                .query((rs, i) -> new ClaimedSeat(rs.getLong(1), rs.getLong(2),
                        rs.getString(3) + " " + rs.getString(4) + "-" + rs.getInt(5)))
                .list();
        if (claimed.size() < count) {
            throw new ApiException(HttpStatus.CONFLICT, "NOT_ENOUGH_SEATS",
                    "Only " + claimed.size() + " seats free in this tier right now", Map.of("tierId", tierId));
        }
        events.publishEvent(new SeatsChanged(showId, SeatsChanged.Change.HELD, claimed.stream().map(ClaimedSeat::seatId).toList()));
        return claimed;
    }

    /** Takes GA units: from the single counter row with the current strategy, or from a random shard. */
    public GaItem claimGa(long showId, long tierId, int quantity, int gaShards) {
        GaItem item;
        if (gaShards > 0) {
            item = new GaItem(tierId, claimFromShard(showId, tierId, quantity, gaShards), quantity);
        } else {
            strategies.get(settings.strategy()).claimGa(showId, tierId, quantity);
            item = new GaItem(tierId, null, quantity);
        }
        events.publishEvent(new GaChanged(showId, tierId));
        return item;
    }

    /**
     * Start at a random shard and walk the ring until one has room. A request is never split
     * across shards, so with multi-ticket requests the last few units can be stranded in shards
     * that each have too few (a known trade-off of sharded counters).
     */
    private int claimFromShard(long showId, long tierId, int quantity, int shards) {
        int start = ThreadLocalRandom.current().nextInt(shards);
        for (int i = 0; i < shards; i++) {
            int shard = (start + i) % shards;
            int updated = jdbc.sql("""
                            UPDATE ga_inventory_shards SET held = held + :q
                            WHERE show_id = :s AND tier_id = :t AND shard = :k AND capacity - held - sold >= :q""")
                    .param("q", quantity).param("s", showId).param("t", tierId).param("k", shard)
                    .update();
            if (updated == 1) {
                return shard;
            }
        }
        throw SeatClaimStrategy.gaSoldOut(tierId);
    }

    /**
     * Gives back what a hold still owns. Seats are matched by {@code hold_id}, never by the hold's
     * item list: with lazy expiry an expired hold's seat may already belong to a newer hold.
     * The caller must already have moved the hold out of HELD (that transition makes this run once).
     */
    public void release(long showId, UUID holdId, List<GaItem> gaItems) {
        List<Long> freed = jdbc.sql("""
                        UPDATE show_seats SET status = 'AVAILABLE', hold_id = NULL, held_until = NULL, version = version + 1
                        WHERE hold_id = :h AND status = 'HELD'
                        RETURNING venue_seat_id""")
                .param("h", holdId)
                .query(Long.class)
                .list();
        for (GaItem item : gaItems) {
            adjustGa(showId, item, "held = held - :q");
            events.publishEvent(new GaChanged(showId, item.tierId()));
        }
        if (!freed.isEmpty()) {
            events.publishEvent(new SeatsChanged(showId, SeatsChanged.Change.RELEASED, freed));
        }
    }

    /**
     * HELD -> BOOKED for every seat of the hold, and held -> sold for its GA units. Returns false
     * (caller must roll back) if a seat no longer belongs to the hold.
     */
    public boolean confirm(long showId, UUID holdId, int expectedSeats, List<GaItem> gaItems) {
        List<Long> booked = jdbc.sql("""
                        UPDATE show_seats SET status = 'BOOKED', held_until = NULL, version = version + 1
                        WHERE hold_id = :h AND status = 'HELD'
                        RETURNING venue_seat_id""")
                .param("h", holdId)
                .query(Long.class)
                .list();
        if (booked.size() != expectedSeats) {
            return false;
        }
        for (GaItem item : gaItems) {
            adjustGa(showId, item, "held = held - :q, sold = sold + :q");
            events.publishEvent(new GaChanged(showId, item.tierId()));
        }
        if (!booked.isEmpty()) {
            events.publishEvent(new SeatsChanged(showId, SeatsChanged.Change.BOOKED, booked));
        }
        return true;
    }

    private void adjustGa(long showId, GaItem item, String set) {
        String sql = item.shard() == null
                ? "UPDATE ga_inventory SET " + set + ", version = version + 1 WHERE show_id = :s AND tier_id = :t"
                : "UPDATE ga_inventory_shards SET " + set + " WHERE show_id = :s AND tier_id = :t AND shard = :k";
        var stmt = jdbc.sql(sql).param("q", item.quantity()).param("s", showId).param("t", item.tierId());
        if (item.shard() != null) {
            stmt = stmt.param("k", item.shard());
        }
        stmt.update();
    }

    private static void checkGapRule(List<RowSeat> rowSeats, List<Long> selected, Instant now) {
        Set<Long> chosen = Set.copyOf(selected);
        boolean anyTaken = rowSeats.stream().anyMatch(s -> chosen.contains(s.seatId()) && s.occupied(now));
        if (anyTaken) {
            return; // the claim reports SEATS_UNAVAILABLE, which is the more useful error
        }
        Set<Long> orphans = GapRule.newOrphans(rowSeats, selected, s -> s.occupied(now));
        if (!orphans.isEmpty()) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, "GAP_RULE_VIOLATION",
                    "This selection would leave a single empty seat", Map.of("orphanSeatIds", List.copyOf(orphans)));
        }
    }

    /** Non-locking strategies: re-read the rows; an orphan nobody chose means two claims combined into one. */
    private void detectGapRace(long showId, List<Long> selected, List<RowSeat> before, Instant now) {
        Set<Long> expected = GapRule.orphans(before, s -> s.occupied(now));
        Set<Long> actual = GapRule.orphans(rows.rowsOf(showId, selected, false), s -> s.occupied(now));
        actual.removeAll(expected);
        if (!actual.isEmpty()) {
            gapRaces.increment(actual.size());
        }
    }
}
