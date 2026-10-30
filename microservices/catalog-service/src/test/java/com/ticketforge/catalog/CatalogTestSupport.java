package com.ticketforge.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.ticketforge.IntegrationTest;

/** Creates small catalog fixtures through the public admin API, like a real admin client would. */
abstract class CatalogTestSupport extends IntegrationTest {

    record Created(long eventId, long venueId, long tierId, long showId) {
    }

    /** A 2-row, 5-seat-per-row venue (10 seats), one event and one show. */
    protected Created createShowFixture(String category) {
        MvcTestResult venue = postJson("/admin/venues", adminToken(), """
                {"name":"Small %s","city":"Pune","tiers":[{"name":"Gold","kind":"RESERVED"}],
                 "sections":[{"name":"M","rows":[{"labels":"A-B","tier":"Gold","seats":5}]}]}""".formatted(category));
        long venueId = ((Number) read(venue, "$.id")).longValue();
        long tierId = ((Number) read(venue, "$.tiers[0].id")).longValue();
        long eventId = createEvent(category);
        MvcTestResult show = postJson("/admin/shows", adminToken(), """
                {"eventId":%d,"venueId":%d,"startsAt":"%s","prices":[{"tierId":%d,"priceMinor":25000}]}"""
                .formatted(eventId, venueId, Instant.now().plus(1, ChronoUnit.DAYS), tierId));
        assertThat(show).hasStatus(201);
        return new Created(eventId, venueId, tierId, ((Number) read(show, "$.id")).longValue());
    }

    protected long createEvent(String category) {
        MvcTestResult r = postJson("/admin/events", adminToken(), """
                {"title":"T-%s","genre":"G","durationMin":120,"category":"%s"}"""
                .formatted(java.util.UUID.randomUUID(), category));
        assertThat(r).hasStatus(201);
        return ((Number) read(r, "$.id")).longValue();
    }
}
