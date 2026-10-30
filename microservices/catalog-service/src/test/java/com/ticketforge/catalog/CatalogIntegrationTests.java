package com.ticketforge.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.ticketforge.IntegrationTest;

class CatalogIntegrationTests extends IntegrationTest {

    @Test
    void venueLayoutExpandsRowsAisleFlagsAndTiers() {
        MvcTestResult r = postJson("/admin/venues", adminToken(), """
                {"name":"Layout Test","city":"Pune",
                 "tiers":[{"name":"Gold","kind":"RESERVED"},{"name":"Floor","kind":"GA","gaCapacity":300}],
                 "sections":[{"name":"MAIN","rows":[{"labels":"A-C","tier":"Gold","seats":10,"aisleAfter":[4]}]}]}""");
        assertThat(r).hasStatus(201).bodyJson().extractingPath("$.seatCount").isEqualTo(30);
        long venueId = ((Number) read(r, "$.id")).longValue();

        Map<String, Object> seat4 = jdbc.sql("""
                        SELECT aisle_left, aisle_right FROM venue_seats
                        WHERE venue_id = :v AND row_label = 'B' AND seat_number = 4""")
                .param("v", venueId).query().singleRow();
        Map<String, Object> seat5 = jdbc.sql("""
                        SELECT aisle_left, aisle_right FROM venue_seats
                        WHERE venue_id = :v AND row_label = 'B' AND seat_number = 5""")
                .param("v", venueId).query().singleRow();
        assertThat(seat4).containsEntry("aisle_right", true).containsEntry("aisle_left", false);
        assertThat(seat5).containsEntry("aisle_left", true);
    }

    @Test
    void gaTierWithoutCapacityIsRejected() {
        assertThat(postJson("/admin/venues", adminToken(), """
                {"name":"Bad","city":"Pune","tiers":[{"name":"Floor","kind":"GA"}],"sections":[]}"""))
                .hasStatus(400).bodyJson().extractingPath("$.code").isEqualTo("INVALID_TIER");
    }

    @Test
    void concertShowGetsConcertDefaultsAndMovieGetsMovieDefaults() {
        long concert = createShow("CONCERT");
        long movie = createShow("MOVIE");

        assertThat(get("/shows/" + concert, null)).hasStatusOk().bodyJson()
                .satisfies(j -> {
                    assertThat(j).extractingPath("$.holdTtlSec").isEqualTo(180);
                    assertThat(j).extractingPath("$.queueEnabled").isEqualTo(true);
                    assertThat(j).extractingPath("$.maxSeatsPerHold").isEqualTo(6);
                    assertThat(j).extractingPath("$.maxHoldsPerUser").isEqualTo(1);
                });
        assertThat(get("/shows/" + movie, null)).hasStatusOk().bodyJson()
                .satisfies(j -> {
                    assertThat(j).extractingPath("$.holdTtlSec").isEqualTo(420);
                    assertThat(j).extractingPath("$.queueEnabled").isEqualTo(false);
                    assertThat(j).extractingPath("$.maxSeatsPerHold").isEqualTo(10);
                    assertThat(j).extractingPath("$.maxHoldsPerUser").isNull();
                });
    }

    @Test
    void showMustPriceEveryTier() {
        MvcTestResult venue = postJson("/admin/venues", adminToken(), """
                {"name":"Two Tiers","city":"Pune","tiers":[{"name":"A","kind":"RESERVED"},{"name":"B","kind":"RESERVED"}],
                 "sections":[{"name":"M","rows":[{"labels":"A","tier":"A","seats":2},{"labels":"B","tier":"B","seats":2}]}]}""");
        long venueId = ((Number) read(venue, "$.id")).longValue();
        List<Integer> tierIds = read(venue, "$.tiers[*].id");
        long eventId = event("MOVIE");

        assertThat(postJson("/admin/shows", adminToken(), """
                {"eventId":%d,"venueId":%d,"startsAt":"%s","prices":[{"tierId":%d,"priceMinor":100}]}"""
                .formatted(eventId, venueId, Instant.now().plus(1, ChronoUnit.DAYS), tierIds.getFirst())))
                .hasStatus(400).bodyJson().extractingPath("$.code").isEqualTo("INVALID_PRICES");
    }

    private long createShow(String category) {
        MvcTestResult venue = postJson("/admin/venues", adminToken(), """
                {"name":"Small %s","city":"Pune","tiers":[{"name":"Gold","kind":"RESERVED"}],
                 "sections":[{"name":"M","rows":[{"labels":"A-B","tier":"Gold","seats":5}]}]}""".formatted(category));
        long venueId = ((Number) read(venue, "$.id")).longValue();
        long tierId = ((Number) read(venue, "$.tiers[0].id")).longValue();
        MvcTestResult show = postJson("/admin/shows", adminToken(), """
                {"eventId":%d,"venueId":%d,"startsAt":"%s","prices":[{"tierId":%d,"priceMinor":25000}]}"""
                .formatted(event(category), venueId, Instant.now().plus(1, ChronoUnit.DAYS), tierId));
        assertThat(show).hasStatus(201);
        return ((Number) read(show, "$.id")).longValue();
    }

    private long event(String category) {
        MvcTestResult r = postJson("/admin/events", adminToken(), """
                {"title":"T","genre":"G","durationMin":120,"category":"%s"}""".formatted(category));
        return ((Number) read(r, "$.id")).longValue();
    }
}
