package com.ticketforge;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.jayway.jsonpath.JsonPath;
import com.ticketforge.catalog.ShowCategory;
import com.ticketforge.contracts.ShowSnapshot;
import com.ticketforge.contracts.VenueLayout;
import com.ticketforge.projection.ShowProjection;
import com.ticketforge.security.TokenService;

/**
 * Base for tests against the whole reservation-service and a real Postgres. Shows are created through
 * {@link ShowProjection}, the same code the Kafka listener runs, fed with a synthetic snapshot and
 * layout instead of a message from catalog-service. One Spring context is shared by all subclasses, so
 * tests create their own shows.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@Import(TestcontainersConfig.class)
public abstract class IntegrationTest {

    /** Catalog ids in real life; here from a counter far above anything another test class would pick. */
    private static final AtomicLong NEXT_SHOW_ID = new AtomicLong(1_000_000);

    @Autowired
    protected MockMvcTester mvc;
    @Autowired
    protected JdbcClient jdbc;
    @Autowired
    protected TestClock clock;
    @Autowired
    protected TokenService tokens;
    @Autowired
    protected ShowProjection projection;
    @org.springframework.boot.test.web.server.LocalServerPort
    protected int port;

    /** A fresh show; {@code seats} are ordered row by row (A1..An, B1..Bn, ...). */
    protected record TestShow(long showId, long reservedTierId, Long gaTierId, List<Long> seats) {
        public long seat(int row, int number, int seatsPerRow) {
            return seats.get(row * seatsPerRow + number - 1);
        }
    }

    /**
     * @param queue           null = the category default
     * @param maxHoldsPerUser null = the category default, 0 = unlimited
     */
    protected TestShow createShow(ShowCategory category, int rows, int seatsPerRow, Integer gaCapacity, Boolean queue,
            Integer maxHoldsPerUser, int gaShards) {
        long showId = NEXT_SHOW_ID.getAndIncrement();
        long gold = showId * 10 + 1;
        Long floor = gaCapacity != null ? showId * 10 + 2 : null;

        List<VenueLayout.Seat> seats = new ArrayList<>();
        for (int r = 0; r < rows; r++) {
            for (int n = 1; n <= seatsPerRow; n++) {
                seats.add(new VenueLayout.Seat(seatId(showId, r * seatsPerRow + n), gold, "MAIN",
                        String.valueOf((char) ('A' + r)), r, n, false, false));
            }
        }
        List<VenueLayout.GaTier> gaTiers = floor == null ? List.of() : List.of(new VenueLayout.GaTier(floor, gaCapacity));

        List<ShowSnapshot.TierPrice> prices = new ArrayList<>(List.of(new ShowSnapshot.TierPrice(gold, 25_000)));
        if (floor != null) {
            prices.add(new ShowSnapshot.TierPrice(floor, 10_000));
        }
        Integer maxHolds = maxHoldsPerUser == null ? category.maxHoldsPerUser : maxHoldsPerUser == 0 ? null : maxHoldsPerUser;
        ShowSnapshot snapshot = new ShowSnapshot(showId, showId, category.name(), clock.instant().plus(Duration.ofDays(30)),
                clock.instant().minusSeconds(60), category.holdTtlSec, queue != null ? queue : category.queueEnabled,
                category.maxSeatsPerHold, maxHolds, category.admitRatePerSec, gaShards, "INR", "ACTIVE", prices);

        projection.apply(snapshot, new VenueLayout(showId, seats, gaTiers));
        return new TestShow(showId, gold, floor, seats.stream().map(VenueLayout.Seat::id).toList());
    }

    /** Distinct per show, and small enough for the tests' (int) casts of JSON numbers. */
    private static long seatId(long showId, int index) {
        return (showId % 1000) * 10_000 + index;
    }

    /** Load-test users have fixed ids; tests use ranges of them as distinct buyers. */
    protected UUID user(int i) {
        return UUID.fromString("00000000-0000-4000-8000-%012d".formatted(i));
    }

    /** Users live in auth-service now; this service only ever sees their ids in a JWT. Kept so the Phase 1 tests read the same. */
    protected void ensureUsers(int count) {
    }

    protected String tokenFor(UUID userId) {
        return tokens.issueAccess(userId, "USER");
    }

    /** A token for a brand-new user. */
    protected String newUser() {
        return tokenFor(UUID.randomUUID());
    }

    protected String adminToken() {
        return tokens.issueAccess(UUID.randomUUID(), "ADMIN");
    }

    /** Every test, whatever it did, must leave this service's invariants intact. */
    @org.junit.jupiter.api.AfterEach
    void invariantsStillHold() {
        MvcTestResult r = get("/admin/invariants", adminToken());
        assertThat(r).hasStatusOk();
        java.util.Map<String, Integer> violations = read(r, "$");
        assertThat(violations).as("invariant violations").allSatisfy((name, count) -> assertThat(count).as(name).isZero());
    }

    protected MvcTestResult postJson(String uri, String token, String body) {
        var req = mvc.post().uri(uri).contentType(MediaType.APPLICATION_JSON).content(body);
        if (token != null) {
            req = req.header("Authorization", "Bearer " + token);
        }
        return req.exchange();
    }

    protected MvcTestResult get(String uri, String token) {
        var req = mvc.get().uri(uri);
        if (token != null) {
            req = req.header("Authorization", "Bearer " + token);
        }
        return req.exchange();
    }

    protected static <T> T read(MvcTestResult result, String path) {
        try {
            return JsonPath.read(result.getResponse().getContentAsString(), path);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    protected static String body(MvcTestResult result) {
        try {
            return result.getResponse().getContentAsString();
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }
}
