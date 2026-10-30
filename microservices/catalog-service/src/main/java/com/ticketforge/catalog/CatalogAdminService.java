package com.ticketforge.catalog;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ticketforge.contracts.ShowSnapshot;
import com.ticketforge.contracts.Topics;
import com.ticketforge.messaging.Outbox;
import com.ticketforge.catalog.CatalogDtos.EventSpec;
import com.ticketforge.catalog.CatalogDtos.EventView;
import com.ticketforge.catalog.CatalogDtos.PriceSpec;
import com.ticketforge.catalog.CatalogDtos.RowSpec;
import com.ticketforge.catalog.CatalogDtos.SectionSpec;
import com.ticketforge.catalog.CatalogDtos.ShowSpec;
import com.ticketforge.catalog.CatalogDtos.TierKind;
import com.ticketforge.catalog.CatalogDtos.TierSpec;
import com.ticketforge.catalog.CatalogDtos.TierView;
import com.ticketforge.catalog.CatalogDtos.VenueCreated;
import com.ticketforge.catalog.CatalogDtos.VenueSpec;
import com.ticketforge.shared.ApiException;
import com.ticketforge.shared.Sql;

@Service
public class CatalogAdminService {

    private static final Logger log = LoggerFactory.getLogger(CatalogAdminService.class);

    private final JdbcClient jdbc;
    private final JdbcTemplate template;
    private final Outbox outbox;
    private final CatalogApi catalog;
    private final Clock clock;

    CatalogAdminService(JdbcClient jdbc, JdbcTemplate template, Outbox outbox, CatalogApi catalog, Clock clock) {
        this.jdbc = jdbc;
        this.template = template;
        this.outbox = outbox;
        this.catalog = catalog;
        this.clock = clock;
    }

    @Transactional
    public VenueCreated createVenue(VenueSpec spec) {
        long venueId = jdbc.sql("INSERT INTO venues (name, city) VALUES (:n, :c) RETURNING id")
                .param("n", spec.name()).param("c", spec.city())
                .query(Long.class).single();

        Map<String, TierView> tiers = new HashMap<>();
        for (TierSpec t : spec.tiers()) {
            boolean ga = t.kind() == TierKind.GA;
            if (ga == (t.gaCapacity() == null)) {
                throw ApiException.badRequest("INVALID_TIER", "Tier '" + t.name() + "': gaCapacity is required for GA tiers only");
            }
            long tierId = jdbc.sql("INSERT INTO seat_tiers (venue_id, name, kind, ga_capacity) VALUES (:v, :n, :k, :cap) RETURNING id")
                    .param("v", venueId).param("n", t.name()).param("k", t.kind().name()).param("cap", t.gaCapacity())
                    .query(Long.class).single();
            if (tiers.put(t.name(), new TierView(tierId, t.name(), t.kind().name(), t.gaCapacity())) != null) {
                throw ApiException.badRequest("INVALID_TIER", "Duplicate tier '" + t.name() + "'");
            }
        }

        List<Object[]> seats = new ArrayList<>();
        Set<String> rowKeys = new HashSet<>();
        for (SectionSpec section : spec.sections() == null ? List.<SectionSpec>of() : spec.sections()) {
            int rowOrder = 0;
            for (RowSpec row : section.rows()) {
                TierView tier = tiers.get(row.tier());
                if (tier == null || !"RESERVED".equals(tier.kind())) {
                    throw ApiException.badRequest("INVALID_TIER", "Row tier '" + row.tier() + "' must be a RESERVED tier");
                }
                Set<Integer> aisles = new HashSet<>(row.aisleAfter() == null ? List.of() : row.aisleAfter());
                for (String label : RowLabels.expand(row.labels())) {
                    if (!rowKeys.add(section.name() + "/" + label)) {
                        throw ApiException.badRequest("DUPLICATE_ROW", "Row " + label + " appears twice in section " + section.name());
                    }
                    rowOrder++;
                    for (int n = 1; n <= row.seats(); n++) {
                        seats.add(new Object[] {venueId, tier.id(), section.name(), label, rowOrder, n,
                                aisles.contains(n - 1), aisles.contains(n)});
                    }
                }
            }
        }
        template.batchUpdate("""
                INSERT INTO venue_seats (venue_id, tier_id, section, row_label, row_order, seat_number, aisle_left, aisle_right)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", seats, 2000, (ps, s) -> {
            for (int i = 0; i < s.length; i++) {
                ps.setObject(i + 1, s[i]);
            }
        });
        log.info("Venue created venue_id={} seats={} tiers={}", venueId, seats.size(), tiers.size());
        return new VenueCreated(venueId, spec.name(), List.copyOf(tiers.values()), seats.size());
    }

    @Transactional
    public EventView createEvent(EventSpec spec) {
        long id = jdbc.sql("INSERT INTO events (title, genre, duration_min, category) VALUES (:t, :g, :d, :c) RETURNING id")
                .param("t", spec.title()).param("g", spec.genre()).param("d", spec.durationMin())
                .param("c", spec.category().name())
                .query(Long.class).single();
        return new EventView(id, spec.title(), spec.genre(), spec.durationMin(), spec.category());
    }

    /**
     * Creates the show with category defaults and publishes it. reservation-service builds the seat
     * inventory when it consumes the event, so inventory appears shortly after the show, not atomically.
     */
    @Transactional
    public long createShow(ShowSpec spec) {
        ShowCategory category = jdbc.sql("SELECT category FROM events WHERE id = :id").param("id", spec.eventId())
                .query(String.class).optional().map(ShowCategory::valueOf)
                .orElseThrow(() -> ApiException.notFound("EVENT_NOT_FOUND", "Event " + spec.eventId() + " not found"));
        Set<Long> venueTiers = new HashSet<>(jdbc.sql("SELECT id FROM seat_tiers WHERE venue_id = :v")
                .param("v", spec.venueId()).query(Long.class).list());
        if (venueTiers.isEmpty()) {
            throw ApiException.notFound("VENUE_NOT_FOUND", "Venue " + spec.venueId() + " not found");
        }
        validatePrices(spec.prices(), venueTiers);

        Instant saleOpens = spec.saleOpensAt() != null ? spec.saleOpensAt() : clock.instant();
        if (saleOpens.isAfter(spec.startsAt())) {
            throw ApiException.badRequest("INVALID_SHOW", "saleOpensAt must not be after startsAt");
        }
        Integer maxHolds = spec.maxHoldsPerUser() == null ? category.maxHoldsPerUser
                : spec.maxHoldsPerUser() == 0 ? null : spec.maxHoldsPerUser();
        int gaShards = spec.gaShards() == null ? 0 : spec.gaShards();

        long showId = jdbc.sql("""
                        INSERT INTO shows (event_id, venue_id, starts_at, sale_opens_at, hold_ttl_sec, queue_enabled,
                                           max_seats_per_hold, max_holds_per_user, admit_rate_per_sec, ga_shards, currency)
                        VALUES (:e, :v, :starts, :sale, :ttl, :queue, :maxSeats, :maxHolds, :rate, :shards, :cur)
                        RETURNING id""")
                .param("e", spec.eventId())
                .param("v", spec.venueId())
                .param("starts", Sql.ts(spec.startsAt()))
                .param("sale", Sql.ts(saleOpens))
                .param("ttl", orDefault(spec.holdTtlSec(), category.holdTtlSec))
                .param("queue", spec.queueEnabled() != null ? spec.queueEnabled() : category.queueEnabled)
                .param("maxSeats", orDefault(spec.maxSeatsPerHold(), category.maxSeatsPerHold))
                .param("maxHolds", maxHolds)
                .param("rate", orDefault(spec.admitRatePerSec(), category.admitRatePerSec))
                .param("shards", gaShards)
                .param("cur", spec.currency() == null ? "INR" : spec.currency())
                .query(Long.class).single();
        insertPrices(showId, spec.prices());

        publish(showId);
        log.info("Show created show_id={} category={} venue_id={}", showId, category, spec.venueId());
        return showId;
    }

    @Transactional
    public void updatePrices(long showId, List<PriceSpec> prices) {
        Long venueId = jdbc.sql("SELECT venue_id FROM shows WHERE id = :id").param("id", showId)
                .query(Long.class).optional()
                .orElseThrow(() -> ApiException.notFound("SHOW_NOT_FOUND", "Show " + showId + " not found"));
        validatePrices(prices, new HashSet<>(jdbc.sql("SELECT id FROM seat_tiers WHERE venue_id = :v")
                .param("v", venueId).query(Long.class).list()));
        jdbc.sql("DELETE FROM show_tier_prices WHERE show_id = :id").param("id", showId).update();
        insertPrices(showId, prices);
        publish(showId);
    }

    /** Writes the show's full snapshot to the outbox, in the caller's transaction. */
    private void publish(long showId) {
        ShowRules r = catalog.rules(showId);
        List<ShowSnapshot.TierPrice> prices = catalog.tierPrices(showId).entrySet().stream()
                .map(e -> new ShowSnapshot.TierPrice(e.getKey(), e.getValue())).toList();
        outbox.add(Topics.CATALOG_SHOWS, Long.toString(showId), Topics.SHOW_PUBLISHED, new ShowSnapshot(
                r.showId(), r.venueId(), r.category().name(), r.startsAt(), r.saleOpensAt(), (int) r.holdTtl().toSeconds(),
                r.queueEnabled(), r.maxSeatsPerHold(), r.maxHoldsPerUser(), r.admitRatePerSec(), r.gaShards(),
                r.currency(), r.active() ? "ACTIVE" : "CANCELLED", prices));
    }

    private void insertPrices(long showId, List<PriceSpec> prices) {
        for (PriceSpec p : prices) {
            jdbc.sql("INSERT INTO show_tier_prices (show_id, tier_id, price_minor) VALUES (:s, :t, :p)")
                    .param("s", showId).param("t", p.tierId()).param("p", p.priceMinor())
                    .update();
        }
    }

    private static void validatePrices(List<PriceSpec> prices, Set<Long> venueTiers) {
        Set<Long> priced = new HashSet<>();
        for (PriceSpec p : prices) {
            if (!venueTiers.contains(p.tierId()) || !priced.add(p.tierId())) {
                throw ApiException.badRequest("INVALID_PRICES", "Tier " + p.tierId() + " is unknown or priced twice");
            }
        }
        if (!priced.equals(venueTiers)) {
            throw ApiException.badRequest("INVALID_PRICES", "Every tier of the venue needs a price");
        }
    }

    private static int orDefault(Integer value, int fallback) {
        return value != null ? value : fallback;
    }
}
