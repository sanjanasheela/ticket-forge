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

/** Read API other modules use. Plain SQL by primary key; no caching in Phase 1 (on purpose). */
@Service
public class CatalogApi {

    private final JdbcClient jdbc;

    CatalogApi(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Optional<ShowRules> findRules(long showId) {
        return jdbc.sql("""
                        SELECT s.id, s.venue_id, e.category, s.starts_at, s.sale_opens_at, s.hold_ttl_sec,
                               s.queue_enabled, s.max_seats_per_hold, s.max_holds_per_user, s.admit_rate_per_sec,
                               s.ga_shards, s.currency, s.status
                        FROM shows s JOIN events e ON e.id = s.event_id
                        WHERE s.id = :id""")
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
        return jdbc.sql("""
                        SELECT s.id, s.venue_id, e.category, s.starts_at, s.sale_opens_at, s.hold_ttl_sec,
                               s.queue_enabled, s.max_seats_per_hold, s.max_holds_per_user, s.admit_rate_per_sec,
                               s.ga_shards, s.currency, s.status
                        FROM shows s JOIN events e ON e.id = s.event_id
                        WHERE s.queue_enabled AND s.status = 'ACTIVE' AND s.sale_opens_at <= :now AND s.starts_at > :now""")
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
