package com.ticketforge;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.jayway.jsonpath.JsonPath;
import com.ticketforge.auth.UserProvisioning;
import com.ticketforge.catalog.CatalogAdminService;
import com.ticketforge.catalog.CatalogDtos.EventSpec;
import com.ticketforge.catalog.CatalogDtos.PriceSpec;
import com.ticketforge.catalog.CatalogDtos.RowSpec;
import com.ticketforge.catalog.CatalogDtos.SectionSpec;
import com.ticketforge.catalog.CatalogDtos.ShowSpec;
import com.ticketforge.catalog.CatalogDtos.TierKind;
import com.ticketforge.catalog.CatalogDtos.TierSpec;
import com.ticketforge.catalog.CatalogDtos.TierView;
import com.ticketforge.catalog.CatalogDtos.VenueCreated;
import com.ticketforge.catalog.CatalogDtos.VenueSpec;
import com.ticketforge.catalog.ShowCategory;

/**
 * Base for tests against the full app and a real Postgres. One Spring context (and one container)
 * is shared by all subclasses, so tests must create their own data (unique emails, fresh shows).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@Import(TestcontainersConfig.class)
public abstract class IntegrationTest {

    @Autowired
    protected MockMvcTester mvc;

    @Autowired
    protected JdbcClient jdbc;

    @Autowired
    protected TestClock clock;

    @org.springframework.boot.test.web.server.LocalServerPort
    protected int port;

    @Autowired
    protected CatalogAdminService catalogAdmin;

    @Autowired
    protected UserProvisioning userProvisioning;

    private String adminToken;

    /** A fresh show; {@code seats} are ordered row by row (A1..An, B1..Bn, ...). */
    protected record TestShow(long showId, long reservedTierId, Long gaTierId, List<Long> seats) {

        public long seat(int row, int number, int seatsPerRow) {
            return seats.get(row * seatsPerRow + number - 1);
        }
    }

    protected TestShow createShow(ShowCategory category, int rows, int seatsPerRow, Integer gaCapacity, Boolean queue,
            Integer maxHoldsPerUser, int gaShards) {
        String name = "Test venue " + UUID.randomUUID();
        List<TierSpec> tiers = new ArrayList<>(List.of(new TierSpec("Gold", TierKind.RESERVED, null)));
        if (gaCapacity != null) {
            tiers.add(new TierSpec("Floor", TierKind.GA, gaCapacity));
        }
        List<SectionSpec> sections = rows == 0 ? List.of() : List.of(new SectionSpec("MAIN",
                List.of(new RowSpec("A-" + (char) ('A' + rows - 1), "Gold", seatsPerRow, List.of()))));
        VenueCreated venue = catalogAdmin.createVenue(new VenueSpec(name, "Test", tiers, sections));
        long gold = venue.tiers().stream().filter(t -> t.name().equals("Gold")).findFirst().orElseThrow().id();
        Long floor = venue.tiers().stream().filter(t -> t.name().equals("Floor")).map(TierView::id).findFirst().orElse(null);
        long event = catalogAdmin.createEvent(new EventSpec("Event", "Test", 120, category)).id();
        List<PriceSpec> prices = new ArrayList<>(List.of(new PriceSpec(gold, 25_000)));
        if (floor != null) {
            prices.add(new PriceSpec(floor, 10_000));
        }
        long showId = catalogAdmin.createShow(new ShowSpec(event, venue.id(), clock.instant().plus(Duration.ofDays(5)),
                clock.instant().minusSeconds(60), "INR", prices, null, queue, null, maxHoldsPerUser, null, gaShards));
        List<Long> seats = jdbc.sql("SELECT venue_seat_id FROM show_seats WHERE show_id = :s ORDER BY row_order, seat_number")
                .param("s", showId).query(Long.class).list();
        return new TestShow(showId, gold, floor, seats);
    }

    /** Load-test users have fixed ids; tests use ranges of them as distinct buyers. */
    protected UUID user(int i) {
        return UserProvisioning.loadTestUserId(i);
    }

    protected void ensureUsers(int count) {
        userProvisioning.ensureLoadTestUsers(count);
    }

    /** Every test, whatever it did, must leave the spec §4 invariants intact. */
    @org.junit.jupiter.api.AfterEach
    void invariantsStillHold() {
        MvcTestResult r = get("/admin/invariants", adminToken());
        assertThat(r).hasStatusOk();
        java.util.Map<String, Integer> violations = read(r, "$");
        assertThat(violations).as("invariant violations").allSatisfy((name, count) -> assertThat(count).as(name).isZero());
    }

    protected String adminToken() {
        if (adminToken == null) {
            adminToken = login("admin@ticketforge.local", "admin12345");
        }
        return adminToken;
    }

    protected String login(String email, String password) {
        MvcTestResult r = postJson("/auth/login", null, """
                {"email":"%s","password":"%s"}""".formatted(email, password));
        assertThat(r).hasStatusOk();
        return read(r, "$.accessToken");
    }

    /** Signs up a fresh user and returns its access token. */
    protected String newUser() {
        MvcTestResult r = postJson("/auth/signup", null, """
                {"email":"user-%s@test.local","password":"password123"}""".formatted(UUID.randomUUID()));
        assertThat(r).hasStatus(201);
        return read(r, "$.accessToken");
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
