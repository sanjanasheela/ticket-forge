package com.ticketforge.catalog;

import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.ticketforge.contracts.VenueLayout;
import com.ticketforge.shared.ApiException;

/**
 * Service-to-service API (guarded by the internal token, never exposed through Kong). reservation-service
 * calls this when it consumes a ShowPublished event to learn the venue's seats.
 */
@RestController
class InternalController {

    private final JdbcClient jdbc;

    InternalController(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/internal/venues/{venueId}/layout")
    VenueLayout layout(@PathVariable long venueId) {
        List<VenueLayout.Seat> seats = jdbc.sql("""
                        SELECT id, tier_id, section, row_label, row_order, seat_number, aisle_left, aisle_right
                        FROM venue_seats WHERE venue_id = :v ORDER BY id""")
                .param("v", venueId)
                .query((rs, i) -> new VenueLayout.Seat(rs.getLong("id"), rs.getLong("tier_id"), rs.getString("section"),
                        rs.getString("row_label"), rs.getInt("row_order"), rs.getInt("seat_number"),
                        rs.getBoolean("aisle_left"), rs.getBoolean("aisle_right")))
                .list();
        List<VenueLayout.GaTier> gaTiers = jdbc.sql(
                        "SELECT id, ga_capacity FROM seat_tiers WHERE venue_id = :v AND kind = 'GA' ORDER BY id")
                .param("v", venueId)
                .query((rs, i) -> new VenueLayout.GaTier(rs.getLong("id"), rs.getInt("ga_capacity")))
                .list();
        if (seats.isEmpty() && gaTiers.isEmpty()) {
            throw ApiException.notFound("VENUE_NOT_FOUND", "Venue " + venueId + " not found");
        }
        return new VenueLayout(venueId, seats, gaTiers);
    }
}
