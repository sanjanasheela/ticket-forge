package com.ticketforge.inventory;

import java.time.Clock;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ticketforge.shared.ApiException;
import com.ticketforge.shared.Sql;

@RestController
class InventoryController {

    /** "S2" before "S10": compare the letters, then the number. */
    private static final java.util.Comparator<String> NATURAL_ORDER = java.util.Comparator
            .comparing((String x) -> x.replaceAll("[0-9]", ""))
            .thenComparingInt(x -> {
                String digits = x.replaceAll("[^0-9]", "");
                return digits.isEmpty() ? 0 : Integer.parseInt(digits);
            });

    private final JdbcClient jdbc;
    private final InventorySettings settings;
    private final Clock clock;

    InventoryController(JdbcClient jdbc, InventorySettings settings, Clock clock) {
        this.jdbc = jdbc;
        this.settings = settings;
        this.clock = clock;
    }

    /**
     * Seat map snapshot. Status letters keep a 50k-seat stadium compact: A(vailable), H(eld), B(ooked).
     * Expired holds already read as A. Pass {@code section} to fetch one stand at a time.
     */
    @GetMapping("/shows/{id}/seats")
    SeatMap seats(@PathVariable long id, @RequestParam(required = false) String section) {
        List<Seat> seats = jdbc.sql("""
                        SELECT venue_seat_id, tier_id, section, row_label, seat_number, aisle_left, aisle_right,
                               CASE WHEN status = 'HELD' AND held_until < :now THEN 'A' ELSE left(status, 1) END AS st
                        FROM show_seats
                        WHERE show_id = :s AND (CAST(:sec AS text) IS NULL OR section = :sec)
                        ORDER BY section, row_order, seat_number""")
                .param("now", Sql.ts(clock.instant())).param("s", id).param("sec", section)
                .query((rs, i) -> new Seat(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getInt(5),
                        rs.getBoolean(6), rs.getBoolean(7), rs.getString(8)))
                .list();
        List<Ga> ga = jdbc.sql("""
                        SELECT g.tier_id, g.capacity,
                               g.capacity - COALESCE(sh.used, g.held + g.sold) AS available
                        FROM ga_inventory g
                        LEFT JOIN (SELECT tier_id, sum(held + sold) AS used FROM ga_inventory_shards
                                   WHERE show_id = :s GROUP BY tier_id) sh ON sh.tier_id = g.tier_id
                        WHERE g.show_id = :s""")
                .param("s", id)
                .query((rs, i) -> new Ga(rs.getLong(1), rs.getInt(2), rs.getInt(3)))
                .list();
        List<String> sections = jdbc.sql("SELECT DISTINCT section FROM show_seats WHERE show_id = :s")
                .param("s", id).query(String.class).list().stream()
                .sorted(NATURAL_ORDER)
                .toList();
        if (sections.isEmpty() && ga.isEmpty()) {
            throw ApiException.notFound("SHOW_NOT_FOUND", "Show " + id + " has no inventory");
        }
        return new SeatMap(id, sections, seats, ga);
    }

    @GetMapping("/admin/inventory/config")
    Config config() {
        return new Config(settings.strategy(), settings.gapRuleEnabled());
    }

    /** Switch the lock strategy / gap rule at runtime, e.g. at the start of a k6 run. */
    @PutMapping("/admin/inventory/config")
    Config update(@RequestBody Config body) {
        if (body.strategy() != null) {
            settings.strategy(body.strategy());
        }
        if (body.gapRuleEnabled() != null) {
            settings.gapRuleEnabled(body.gapRuleEnabled());
        }
        return config();
    }

    record Seat(long id, long tier, String sec, String row, int n, boolean al, boolean ar, String st) {
    }

    record Ga(long tierId, int capacity, int available) {
    }

    record SeatMap(long showId, List<String> sections, List<Seat> seats, List<Ga> ga) {
    }

    record Config(LockStrategy strategy, Boolean gapRuleEnabled) {
    }
}
