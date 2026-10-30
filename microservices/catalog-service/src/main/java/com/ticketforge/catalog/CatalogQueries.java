package com.ticketforge.catalog;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import com.ticketforge.catalog.CatalogDtos.EventView;
import com.ticketforge.catalog.CatalogDtos.ShowDetails;
import com.ticketforge.catalog.CatalogDtos.ShowSummary;
import com.ticketforge.catalog.CatalogDtos.ShowTier;
import com.ticketforge.shared.ApiException;

@Service
class CatalogQueries {

    private static final String SHOW_SUMMARY = """
            SELECT s.id, s.event_id, e.title, e.category, s.venue_id, v.name AS venue_name, v.city,
                   s.starts_at, s.sale_opens_at
            FROM shows s JOIN events e ON e.id = s.event_id JOIN venues v ON v.id = s.venue_id
            """;

    private final JdbcClient jdbc;

    CatalogQueries(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    List<EventView> events(ShowCategory category) {
        return jdbc.sql("""
                        SELECT id, title, genre, duration_min, category FROM events
                        WHERE (CAST(:cat AS text) IS NULL OR category = :cat) ORDER BY id""")
                .param("cat", category == null ? null : category.name())
                .query((rs, i) -> new EventView(rs.getLong("id"), rs.getString("title"), rs.getString("genre"),
                        rs.getInt("duration_min"), ShowCategory.valueOf(rs.getString("category"))))
                .list();
    }

    List<ShowSummary> showsOfEvent(long eventId) {
        return jdbc.sql(SHOW_SUMMARY + " WHERE s.event_id = :e AND s.status = 'ACTIVE' ORDER BY s.starts_at")
                .param("e", eventId)
                .query(CatalogQueries::summary)
                .list();
    }

    ShowDetails show(long showId) {
        ShowSummary summary = jdbc.sql(SHOW_SUMMARY + " WHERE s.id = :id").param("id", showId)
                .query(CatalogQueries::summary).optional()
                .orElseThrow(() -> ApiException.notFound("SHOW_NOT_FOUND", "Show " + showId + " not found"));
        List<ShowTier> tiers = jdbc.sql("""
                        SELECT t.id, t.name, t.kind, t.ga_capacity, p.price_minor
                        FROM seat_tiers t JOIN show_tier_prices p ON p.tier_id = t.id AND p.show_id = :id
                        ORDER BY p.price_minor DESC""")
                .param("id", showId)
                .query((rs, i) -> new ShowTier(rs.getLong("id"), rs.getString("name"), rs.getString("kind"),
                        rs.getObject("ga_capacity", Integer.class), rs.getLong("price_minor")))
                .list();
        return jdbc.sql("""
                        SELECT currency, hold_ttl_sec, queue_enabled, max_seats_per_hold, max_holds_per_user, ga_shards
                        FROM shows WHERE id = :id""")
                .param("id", showId)
                .query((rs, i) -> new ShowDetails(summary, rs.getString("currency"), rs.getInt("hold_ttl_sec"),
                        rs.getBoolean("queue_enabled"), rs.getInt("max_seats_per_hold"),
                        rs.getObject("max_holds_per_user", Integer.class), rs.getInt("ga_shards"), tiers))
                .single();
    }

    private static ShowSummary summary(ResultSet rs, int row) throws SQLException {
        return new ShowSummary(rs.getLong("id"), rs.getLong("event_id"), rs.getString("title"),
                ShowCategory.valueOf(rs.getString("category")), rs.getLong("venue_id"), rs.getString("venue_name"),
                rs.getString("city"), rs.getTimestamp("starts_at").toInstant(), rs.getTimestamp("sale_opens_at").toInstant());
    }
}
