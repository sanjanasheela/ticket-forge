package com.ticketforge.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

/** /internal/** is for other services only: a missing or wrong token is refused, a user's JWT is not enough. */
class InternalApiTests extends CatalogTestSupport {

    private static final String TOKEN_HEADER = "X-Internal-Token";
    private static final String DEV_TOKEN = "dev-internal-token-change-me";

    @Test
    void layoutNeedsTheInternalToken() {
        Created c = createShowFixture("MOVIE");
        String uri = "/internal/venues/" + c.venueId() + "/layout";

        assertThat(mvc.get().uri(uri).exchange()).hasStatus(403);
        assertThat(mvc.get().uri(uri).header(TOKEN_HEADER, "wrong").exchange()).hasStatus(403);
        assertThat(get(uri, adminToken())).as("an admin's JWT is not a service credential").hasStatus(403);
    }

    @Test
    void layoutListsEverySeatOfTheVenue() {
        Created c = createShowFixture("MOVIE");

        MvcTestResult r = mvc.get().uri("/internal/venues/" + c.venueId() + "/layout").header(TOKEN_HEADER, DEV_TOKEN).exchange();

        assertThat(r).hasStatusOk();
        assertThat(r).bodyJson().extractingPath("$.seats.length()").isEqualTo(10);
        assertThat(r).bodyJson().extractingPath("$.seats[0].tierId").isEqualTo((int) c.tierId());
        assertThat(r).bodyJson().extractingPath("$.gaTiers.length()").isEqualTo(0);
    }

    @Test
    void unknownVenueIs404() {
        assertThat(mvc.get().uri("/internal/venues/999999999/layout").header(TOKEN_HEADER, DEV_TOKEN).exchange()).hasStatus(404);
    }
}
