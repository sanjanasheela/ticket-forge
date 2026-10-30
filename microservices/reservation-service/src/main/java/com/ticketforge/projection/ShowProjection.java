package com.ticketforge.projection;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ticketforge.contracts.ShowSnapshot;
import com.ticketforge.contracts.VenueLayout;
import com.ticketforge.shared.Sql;

/**
 * Keeps reservation-service's local copy of the catalog current: the show rules and prices, and the
 * seat inventory built from the venue layout. This is what the monolith's synchronous
 * {@code InventoryMaterializer} became once catalog and reservation stopped sharing a database.
 *
 * <p>Both steps are idempotent, because Kafka delivers at least once and a failed attempt is retried:
 * the rules are an upsert of a full snapshot, and the inventory is built at most once per show
 * ({@code ON CONFLICT DO NOTHING}).
 */
@Service
public class ShowProjection {

    private static final Logger log = LoggerFactory.getLogger(ShowProjection.class);

    private final JdbcClient jdbc;
    private final JdbcTemplate template;

    ShowProjection(JdbcClient jdbc, JdbcTemplate template) {
        this.jdbc = jdbc;
        this.template = template;
    }

    /** Step 1: show rules and prices. Never touches {@code inventory_ready}, so a price change cannot "unbuild" inventory. */
    @Transactional
    public void upsertShow(ShowSnapshot s) {
        jdbc.sql("""
                        INSERT INTO shows (id, venue_id, category, starts_at, sale_opens_at, hold_ttl_sec, queue_enabled,
                                           max_seats_per_hold, max_holds_per_user, admit_rate_per_sec, ga_shards, currency, status)
                        VALUES (:id, :venue, :cat, :starts, :sale, :ttl, :queue, :maxSeats, :maxHolds, :rate, :shards, :cur, :status)
                        ON CONFLICT (id) DO UPDATE SET
                            venue_id = EXCLUDED.venue_id, category = EXCLUDED.category, starts_at = EXCLUDED.starts_at,
                            sale_opens_at = EXCLUDED.sale_opens_at, hold_ttl_sec = EXCLUDED.hold_ttl_sec,
                            queue_enabled = EXCLUDED.queue_enabled, max_seats_per_hold = EXCLUDED.max_seats_per_hold,
                            max_holds_per_user = EXCLUDED.max_holds_per_user, admit_rate_per_sec = EXCLUDED.admit_rate_per_sec,
                            currency = EXCLUDED.currency, status = EXCLUDED.status, updated_at = now()""")
                .param("id", s.showId()).param("venue", s.venueId()).param("cat", s.category())
                .param("starts", Sql.ts(s.startsAt())).param("sale", Sql.ts(s.saleOpensAt()))
                .param("ttl", s.holdTtlSec()).param("queue", s.queueEnabled()).param("maxSeats", s.maxSeatsPerHold())
                .param("maxHolds", s.maxHoldsPerUser()).param("rate", s.admitRatePerSec())
                .param("shards", s.gaShards()).param("cur", s.currency()).param("status", s.status())
                .update();
        jdbc.sql("DELETE FROM show_tier_prices WHERE show_id = :id").param("id", s.showId()).update();
        for (ShowSnapshot.TierPrice p : s.prices()) {
            jdbc.sql("INSERT INTO show_tier_prices (show_id, tier_id, price_minor) VALUES (:s, :t, :p)")
                    .param("s", s.showId()).param("t", p.tierId()).param("p", p.priceMinor()).update();
        }
    }

    public boolean inventoryReady(long showId) {
        return jdbc.sql("SELECT inventory_ready FROM shows WHERE id = :id").param("id", showId)
                .query(Boolean.class).optional().orElse(false);
    }

    /**
     * Step 2: build the show's seats and GA counters from the venue layout, then mark the show
     * bookable. One transaction: a show is either fully built or invisible to the booking path.
     */
    @Transactional
    public void materialiseInventory(ShowSnapshot s, VenueLayout layout) {
        if (inventoryReady(s.showId())) {
            return;
        }
        List<VenueLayout.Seat> seats = layout.seats();
        int inserted = template.execute((Connection con) -> {
            try (PreparedStatement ps = con.prepareStatement("""
                    INSERT INTO show_seats (show_id, venue_seat_id, tier_id, section, row_label, row_order, seat_number,
                                            aisle_left, aisle_right)
                    SELECT ?, * FROM unnest(?::bigint[], ?::bigint[], ?::text[], ?::text[], ?::int[], ?::int[], ?::boolean[], ?::boolean[])
                    ON CONFLICT DO NOTHING""")) {
                ps.setLong(1, s.showId());
                ps.setArray(2, array(con, "bigint", seats.stream().map(VenueLayout.Seat::id).toArray()));
                ps.setArray(3, array(con, "bigint", seats.stream().map(VenueLayout.Seat::tierId).toArray()));
                ps.setArray(4, array(con, "text", seats.stream().map(VenueLayout.Seat::section).toArray()));
                ps.setArray(5, array(con, "text", seats.stream().map(VenueLayout.Seat::rowLabel).toArray()));
                ps.setArray(6, array(con, "int", seats.stream().map(VenueLayout.Seat::rowOrder).toArray()));
                ps.setArray(7, array(con, "int", seats.stream().map(VenueLayout.Seat::seatNumber).toArray()));
                ps.setArray(8, array(con, "boolean", seats.stream().map(VenueLayout.Seat::aisleLeft).toArray()));
                ps.setArray(9, array(con, "boolean", seats.stream().map(VenueLayout.Seat::aisleRight).toArray()));
                return ps.executeUpdate();
            }
        });

        for (VenueLayout.GaTier tier : layout.gaTiers()) {
            jdbc.sql("INSERT INTO ga_inventory (show_id, tier_id, capacity) VALUES (:s, :t, :c) ON CONFLICT DO NOTHING")
                    .param("s", s.showId()).param("t", tier.tierId()).param("c", tier.capacity()).update();
            if (s.gaShards() > 0) {
                // capacity / K per shard, the remainder spread over the first shards
                jdbc.sql("""
                                INSERT INTO ga_inventory_shards (show_id, tier_id, shard, capacity)
                                SELECT :s, :t, g, :c / :k + CASE WHEN g < :c % :k THEN 1 ELSE 0 END
                                FROM generate_series(0, :k - 1) AS g
                                ON CONFLICT DO NOTHING""")
                        .param("s", s.showId()).param("t", tier.tierId()).param("c", tier.capacity()).param("k", s.gaShards())
                        .update();
            }
        }
        // Fresh statistics: without them the planner believes a brand-new show has ~1 seat (the Phase 1 outage, baseline report 4.1).
        jdbc.sql("ANALYZE show_seats").update();
        jdbc.sql("ANALYZE ga_inventory").update();
        jdbc.sql("UPDATE shows SET inventory_ready = true, updated_at = now() WHERE id = :id").param("id", s.showId()).update();
        log.info("Inventory materialised show_id={} seats={} ga_tiers={} ga_shards={}", s.showId(), inserted,
                layout.gaTiers().size(), s.gaShards());
    }

    /** Both steps in one call: what the Kafka listener does, minus the HTTP fetch. Used by tests and tooling. */
    @Transactional
    public void apply(ShowSnapshot s, VenueLayout layout) {
        upsertShow(s);
        materialiseInventory(s, layout);
    }

    private static Array array(Connection con, String type, Object[] values) throws java.sql.SQLException {
        return con.createArrayOf(type, values);
    }
}
