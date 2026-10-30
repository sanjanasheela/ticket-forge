package com.ticketforge.catalog;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import com.ticketforge.shared.ApiException;
import com.ticketforge.shared.Sql;

/**
 * What the booking path knows about a show. In Phase 1 this queried the catalog's own tables; in
 * Phase 2 it reads reservation-service's local read-model (tables {@code shows} and
 * {@code show_tier_prices}), which {@code ShowProjection} keeps up to date from catalog events.
 * Same class name and method signatures as before, so the inventory and reservation code is unchanged.
 */
@Service
public class CatalogApi {

    private static final String RULES = """
            SELECT id, venue_id, category, starts_at, sale_opens_at, hold_ttl_sec, queue_enabled, max_seats_per_hold,
                   max_holds_per_user, admit_rate_per_sec, ga_shards, currency, status
            FROM shows""";

    private final JdbcClient jdbc;

    CatalogApi(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** Only shows whose inventory has been built are bookable; until then they simply do not exist here. */
    public Optional<ShowRules> findRules(long showId) {
        return jdbc.sql(RULES + " WHERE id = :id AND inventory_ready")
                .param("id", showId)
                .query(CatalogApi::rules)
                .optional();
    }

    public ShowRules rules(long showId) {
        return findRules(showId).orElseThrow(() -> ApiException.notFound("SHOW_NOT_FOUND", "Show " + showId + " not found"));
    }

    /** tier id -> price in minor units. */
    public Map<Long, Long> tierPrices(long showId) {
        Map<Long, Long> prices = new HashMap<>();
        jdbc.sql("SELECT tier_id, price_minor FROM show_tier_prices WHERE show_id = :id")
                .param("id", showId)
                .query(rs -> {
                    prices.put(rs.getLong(1), rs.getLong(2));
                });
        return prices;
    }

    /** Shows whose waiting room is on and whose sale is open or about to open. */
    public List<ShowRules> queueEnabledShowsOnSale(java.time.Instant now) {
        return jdbc.sql(RULES + """

                         WHERE queue_enabled AND status = 'ACTIVE' AND inventory_ready
                           AND sale_opens_at <= :now AND starts_at > :now""")
                .param("now", Sql.ts(now))
                .query(CatalogApi::rules)
                .list();
    }

    private static ShowRules rules(ResultSet rs, int row) throws SQLException {
        return new ShowRules(
                rs.getLong("id"),
                rs.getLong("venue_id"),
                ShowCategory.valueOf(rs.getString("category")),
                rs.getTimestamp("starts_at").toInstant(),
                rs.getTimestamp("sale_opens_at").toInstant(),
                Duration.ofSeconds(rs.getInt("hold_ttl_sec")),
                rs.getBoolean("queue_enabled"),
                rs.getInt("max_seats_per_hold"),
                rs.getObject("max_holds_per_user", Integer.class),
                rs.getInt("admit_rate_per_sec"),
                rs.getInt("ga_shards"),
                rs.getString("currency"),
                "ACTIVE".equals(rs.getString("status")));
    }
}
