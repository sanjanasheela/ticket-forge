package com.ticketforge.seed;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import com.ticketforge.auth.UserProvisioning;
import com.ticketforge.catalog.CatalogAdminService;
import com.ticketforge.catalog.CatalogDtos.EventSpec;
import com.ticketforge.catalog.CatalogDtos.PriceSpec;
import com.ticketforge.catalog.CatalogDtos.RowSpec;
import com.ticketforge.catalog.CatalogDtos.SectionSpec;
import com.ticketforge.catalog.CatalogDtos.ShowSpec;
import com.ticketforge.catalog.CatalogDtos.TierKind;
import com.ticketforge.catalog.CatalogDtos.TierSpec;
import com.ticketforge.catalog.CatalogDtos.VenueSpec;
import com.ticketforge.catalog.ShowCategory;

/** Demo and load-test data. Every operation is safe to call repeatedly. */
@Service
class SeedService {

    private static final Logger log = LoggerFactory.getLogger(SeedService.class);

    static final String STADIUM = "Forge Arena";
    static final String MULTIPLEX = "Forge Cinemas";
    private static final int SCREENS = 10;

    /** Prices in paise. */
    private static final Map<String, Long> STADIUM_PRICES = Map.of(
            "Platinum", 1_500_000L, "Gold", 800_000L, "Silver", 350_000L, "Pit", 600_000L);
    private static final Map<String, Long> SCREEN_PRICES = Map.of(
            "Recliner", 60_000L, "Gold", 30_000L, "Silver", 18_000L);

    private final CatalogAdminService catalog;
    private final UserProvisioning users;
    private final JdbcClient jdbc;
    private final Clock clock;

    SeedService(CatalogAdminService catalog, UserProvisioning users, JdbcClient jdbc, Clock clock) {
        this.catalog = catalog;
        this.users = users;
        this.jdbc = jdbc;
        this.clock = clock;
    }

    record DemoResult(long stadiumVenueId, long concertShowId, List<Long> screenVenueIds, int movieShows) {
    }

    DemoResult demo() {
        long stadium = venueId(STADIUM).orElseGet(() -> catalog.createVenue(stadiumSpec()).id());
        long concertEvent = eventId("The Forge World Tour")
                .orElseGet(() -> catalog.createEvent(new EventSpec("The Forge World Tour", "Pop", 180, ShowCategory.CONCERT)).id());
        long concertShow = firstShow(concertEvent).orElseGet(() -> concertShow(new ConcertOptions(null, null, null, null, 0)));

        List<Long> screens = new ArrayList<>();
        for (int i = 1; i <= SCREENS; i++) {
            String name = MULTIPLEX + " - Screen " + i;
            screens.add(venueId(name).orElseGet(() -> catalog.createVenue(screenSpec(name)).id()));
        }
        String[][] movies = {
                {"Interstellar Drift", "Sci-Fi", "169"}, {"The Last Monsoon", "Drama", "142"},
                {"Byte Club", "Thriller", "121"}, {"Laugh Track", "Comedy", "104"}, {"Dragon's Ledger", "Fantasy", "155"}};
        List<Long> movieEvents = new ArrayList<>();
        for (String[] m : movies) {
            movieEvents.add(eventId(m[0]).orElseGet(() ->
                    catalog.createEvent(new EventSpec(m[0], m[1], Integer.parseInt(m[2]), ShowCategory.MOVIE)).id()));
        }

        int created = 0;
        if (firstShow(movieEvents.getFirst()).isEmpty()) {
            Instant tomorrow = clock.instant().truncatedTo(ChronoUnit.DAYS).plus(Duration.ofDays(1));
            int[] startHours = {10, 14, 18, 22};
            for (int day = 0; day < 2; day++) {
                for (int s = 0; s < SCREENS; s++) {
                    for (int slot = 0; slot < startHours.length; slot++) {
                        long event = movieEvents.get((s + slot + day) % movieEvents.size());
                        Instant starts = tomorrow.plus(Duration.ofDays(day)).plus(Duration.ofHours(startHours[slot]));
                        catalog.createShow(showSpec(event, screens.get(s), starts, clock.instant(), SCREEN_PRICES,
                                null, null, null, null));
                        created++;
                    }
                }
            }
        }
        log.info("Demo seed done stadium={} concert_show={} movie_shows_created={}", stadium, concertShow, created);
        return new DemoResult(stadium, concertShow, screens, created);
    }

    /** Options for a fresh concert show; nulls use the CONCERT defaults. */
    record ConcertOptions(Boolean queue, Integer holdTtlSec, Integer maxHoldsPerUser, Integer saleOpensInSec, int gaShards) {
    }

    /** A brand-new concert show on the stadium, so every load-test run starts from full inventory. */
    long concertShow(ConcertOptions o) {
        long stadium = venueId(STADIUM).orElseGet(() -> catalog.createVenue(stadiumSpec()).id());
        long event = eventId("The Forge World Tour")
                .orElseGet(() -> catalog.createEvent(new EventSpec("The Forge World Tour", "Pop", 180, ShowCategory.CONCERT)).id());
        Instant now = clock.instant();
        Instant saleOpens = o.saleOpensInSec() == null ? now : now.plusSeconds(o.saleOpensInSec());
        return catalog.createShow(showSpec(event, stadium, now.plus(Duration.ofDays(30)), saleOpens, STADIUM_PRICES,
                o.queue(), o.holdTtlSec(), o.maxHoldsPerUser(), o.gaShards()));
    }

    /** {@code count} fresh movie shows spread over the multiplex screens (Friday-evening load test). */
    List<Long> movieShows(int count) {
        demo();
        List<Long> screens = new ArrayList<>();
        for (int i = 1; i <= SCREENS; i++) {
            screens.add(venueId(MULTIPLEX + " - Screen " + i).orElseThrow());
        }
        List<Long> movies = jdbc.sql("SELECT id FROM events WHERE category = 'MOVIE' ORDER BY id").query(Long.class).list();
        Instant base = clock.instant().plus(Duration.ofDays(3));
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add(catalog.createShow(showSpec(movies.get(i % movies.size()), screens.get(i % screens.size()),
                    base.plus(Duration.ofMinutes(i)), clock.instant(), SCREEN_PRICES, null, null, null, null)));
        }
        return ids;
    }

    int loadTestUsers(int count) {
        return users.ensureLoadTestUsers(count);
    }

    // ---- layouts -----------------------------------------------------------------------------

    /** 20 sections x 50 rows x 50 seats = 50,000 reserved seats, plus a 10,000 standing pit. */
    private static VenueSpec stadiumSpec() {
        List<SectionSpec> sections = new ArrayList<>();
        for (int s = 1; s <= 20; s++) {
            String tier = s <= 4 ? "Platinum" : s <= 12 ? "Gold" : "Silver";
            sections.add(new SectionSpec("S" + s, List.of(new RowSpec("1-50", tier, 50, List.of(25)))));
        }
        return new VenueSpec(STADIUM, "Hyderabad", List.of(
                new TierSpec("Platinum", TierKind.RESERVED, null),
                new TierSpec("Gold", TierKind.RESERVED, null),
                new TierSpec("Silver", TierKind.RESERVED, null),
                new TierSpec("Pit", TierKind.GA, 10_000)), sections);
    }

    /** A typical Indian multiplex screen: 232 seats, recliners at the back. */
    private static VenueSpec screenSpec(String name) {
        return new VenueSpec(name, "Hyderabad", List.of(
                new TierSpec("Recliner", TierKind.RESERVED, null),
                new TierSpec("Gold", TierKind.RESERVED, null),
                new TierSpec("Silver", TierKind.RESERVED, null)),
                List.of(new SectionSpec("MAIN", List.of(
                        new RowSpec("A-D", "Silver", 20, List.of(5, 15)),
                        new RowSpec("E-J", "Gold", 20, List.of(5, 15)),
                        new RowSpec("K-L", "Recliner", 16, List.of(8))))));
    }

    private ShowSpec showSpec(long eventId, long venueId, Instant starts, Instant saleOpens, Map<String, Long> prices,
            Boolean queue, Integer ttl, Integer maxHolds, Integer gaShards) {
        Map<String, Long> tierIds = jdbc.sql("SELECT name, id FROM seat_tiers WHERE venue_id = :v").param("v", venueId)
                .query((rs, i) -> Map.entry(rs.getString(1), rs.getLong(2))).list().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        List<PriceSpec> priceSpecs = tierIds.entrySet().stream()
                .map(t -> new PriceSpec(t.getValue(), prices.get(t.getKey())))
                .toList();
        return new ShowSpec(eventId, venueId, starts, saleOpens, "INR", priceSpecs, ttl, queue, null, maxHolds, null, gaShards);
    }

    private Optional<Long> venueId(String name) {
        return jdbc.sql("SELECT id FROM venues WHERE name = :n ORDER BY id LIMIT 1").param("n", name).query(Long.class).optional();
    }

    private Optional<Long> eventId(String title) {
        return jdbc.sql("SELECT id FROM events WHERE title = :t ORDER BY id LIMIT 1").param("t", title).query(Long.class).optional();
    }

    private Optional<Long> firstShow(long eventId) {
        return jdbc.sql("SELECT id FROM shows WHERE event_id = :e ORDER BY id LIMIT 1").param("e", eventId).query(Long.class).optional();
    }
}
