package com.ticketforge.inventory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import com.ticketforge.catalog.ShowCreated;

/**
 * Builds a show's inventory in the show-creation transaction. Reads the catalog's layout tables
 * directly with INSERT ... SELECT: 50k seats in one statement. Acceptable inside one monolith
 * database; in Phase 2 the event would carry the layout instead.
 */
@Component
class InventoryMaterializer {

    private static final Logger log = LoggerFactory.getLogger(InventoryMaterializer.class);

    private final JdbcClient jdbc;

    InventoryMaterializer(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @EventListener
    void on(ShowCreated show) {
        int seats = jdbc.sql("""
                        INSERT INTO show_seats (show_id, venue_seat_id, tier_id, section, row_label, row_order, seat_number,
                                                aisle_left, aisle_right)
                        SELECT :s, id, tier_id, section, row_label, row_order, seat_number, aisle_left, aisle_right
                        FROM venue_seats WHERE venue_id = :v""")
                .param("s", show.showId()).param("v", show.venueId())
                .update();
        jdbc.sql("""
                        INSERT INTO ga_inventory (show_id, tier_id, capacity)
                        SELECT :s, id, ga_capacity FROM seat_tiers WHERE venue_id = :v AND kind = 'GA'""")
                .param("s", show.showId()).param("v", show.venueId())
                .update();
        if (show.gaShards() > 0) {
            // capacity / K per shard, the remainder spread over the first shards
            jdbc.sql("""
                            INSERT INTO ga_inventory_shards (show_id, tier_id, shard, capacity)
                            SELECT :s, t.id, g, t.ga_capacity / :k + CASE WHEN g < t.ga_capacity % :k THEN 1 ELSE 0 END
                            FROM seat_tiers t CROSS JOIN generate_series(0, :k - 1) AS g
                            WHERE t.venue_id = :v AND t.kind = 'GA'""")
                    .param("s", show.showId()).param("v", show.venueId()).param("k", show.gaShards())
                    .update();
        }
        // Fresh statistics: without them the planner believes a brand-new show has ~1 seat.
        jdbc.sql("ANALYZE show_seats").update();
        jdbc.sql("ANALYZE ga_inventory").update();
        log.info("Inventory materialised show_id={} seats={} ga_shards={}", show.showId(), seats, show.gaShards());
    }
}
