package com.ticketforge.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.ticketforge.IntegrationTest;
import com.ticketforge.security.TokenService;
import com.ticketforge.catalog.ShowCategory;
import com.ticketforge.reservation.HoldViews.HoldRequest;
import com.ticketforge.reservation.HoldViews.HoldView;
import com.ticketforge.shared.ApiException;

class ReservationIntegrationTests extends IntegrationTest {

    @Autowired
    ReservationService reservations;

    @Autowired
    TokenService tokens;

    @BeforeEach
    void users() {
        ensureUsers(10);
    }

    @Test
    void holdOverHttpPricesSeatsAndShowsUpInSeatMap() {
        TestShow show = createShow(ShowCategory.MOVIE, 2, 10, null, false, 0, 0);
        String token = newUser();
        long a4 = show.seat(0, 4, 10);
        long a5 = show.seat(0, 5, 10);

        MvcTestResult r = postJson("/shows/" + show.showId() + "/holds", token, "{\"seatIds\":[%d,%d]}".formatted(a4, a5));

        assertThat(r).hasStatus(201).bodyJson().satisfies(j -> {
            assertThat(j).extractingPath("$.status").isEqualTo("HELD");
            assertThat(j).extractingPath("$.totalMinor").isEqualTo(50_000);
            assertThat(j).extractingPath("$.items[0].label").isEqualTo("MAIN A-4");
        });
        MvcTestResult map = get("/shows/" + show.showId() + "/seats", null);
        List<String> statuses = read(map, "$.seats[?(@.id in [%d,%d])].st".formatted(a4, a5));
        assertThat(statuses).containsExactly("H", "H");

        // a second user loses with the conflicting seats listed
        assertThat(postJson("/shows/" + show.showId() + "/holds", newUser(), "{\"seatIds\":[%d]}".formatted(a5)))
                .hasStatus(409).bodyJson().extractingPath("$.details.seatIds[0]").isEqualTo((int) a5);
    }

    @Test
    void gapRuleRejectsLeavingASingleSeat() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        assertThat(postJson("/shows/" + show.showId() + "/holds", newUser(),
                "{\"seatIds\":[%d,%d]}".formatted(show.seat(0, 2, 10), show.seat(0, 3, 10))))
                .hasStatus(422).bodyJson().extractingPath("$.code").isEqualTo("GAP_RULE_VIOLATION");
    }

    @Test
    void maxSeatsPerHoldIsEnforced() {
        TestShow show = createShow(ShowCategory.CONCERT, 1, 10, null, false, 0, 0);
        assertThatThrownBy(() -> reservations.createHold(user(1), show.showId(),
                new HoldRequest(show.seats().subList(0, 7), null, null, null), null))
                .isInstanceOf(ApiException.class).extracting("code").isEqualTo("MAX_SEATS_EXCEEDED");
    }

    @Test
    void concertAllowsOneActiveHoldPerUser() {
        TestShow show = createShow(ShowCategory.CONCERT, 1, 10, null, false, null, 0);
        reservations.createHold(user(2), show.showId(), new HoldRequest(List.of(show.seat(0, 1, 10)), null, null, null), null);

        assertThatThrownBy(() -> reservations.createHold(user(2), show.showId(),
                new HoldRequest(List.of(show.seat(0, 5, 10)), null, null, null), null))
                .isInstanceOf(ApiException.class).extracting("code").isEqualTo("HOLD_LIMIT_REACHED");
    }

    @Test
    void ownExpiredButUnsweptHoldDoesNotBlockTheNextOne() {
        TestShow show = createShow(ShowCategory.CONCERT, 1, 10, null, false, null, 0);
        reservations.createHold(user(3), show.showId(), new HoldRequest(List.of(show.seat(0, 1, 10)), null, null, null), null);
        clock.advance(Duration.ofMinutes(4)); // concert TTL is 3 min

        HoldView second = reservations.createHold(user(3), show.showId(),
                new HoldRequest(List.of(show.seat(0, 5, 10)), null, null, null), null);
        assertThat(second.status()).isEqualTo("HELD");
    }

    @Test
    void queueEnabledShowRequiresAValidAccessToken() {
        TestShow show = createShow(ShowCategory.CONCERT, 1, 10, null, true, null, 0);
        HoldRequest req = new HoldRequest(List.of(show.seat(0, 1, 10)), null, null, null);

        assertThatThrownBy(() -> reservations.createHold(user(4), show.showId(), req, null))
                .isInstanceOf(ApiException.class).extracting("code").isEqualTo("QUEUE_REQUIRED");
        String othersToken = tokens.issueQueueAccess(user(5), show.showId(), Duration.ofMinutes(5)).token();
        assertThatThrownBy(() -> reservations.createHold(user(4), show.showId(), req, othersToken))
                .isInstanceOf(ApiException.class).extracting("code").isEqualTo("QUEUE_REQUIRED");

        String mine = tokens.issueQueueAccess(user(4), show.showId(), Duration.ofMinutes(5)).token();
        assertThat(reservations.createHold(user(4), show.showId(), req, mine).status()).isEqualTo("HELD");
    }

    @Test
    void releaseReturnsSeatsAndGaUnits() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, 100, false, 0, 0);
        HoldView seats = reservations.createHold(user(6), show.showId(),
                new HoldRequest(List.of(show.seat(0, 1, 10), show.seat(0, 2, 10)), null, null, null), null);
        HoldView ga = reservations.createHold(user(6), show.showId(), new HoldRequest(null, show.gaTierId(), 4, null), null);

        reservations.release(seats.holdId(), user(6));
        reservations.release(ga.holdId(), user(6));

        assertThat(statusOf(show.showId(), show.seat(0, 1, 10))).isEqualTo("AVAILABLE");
        assertThat(gaHeld(show.showId())).isZero();
        assertThatThrownBy(() -> reservations.release(seats.holdId(), user(6)))
                .isInstanceOf(ApiException.class).extracting("code").isEqualTo("HOLD_NOT_ACTIVE");
    }

    @Test
    void sweeperExpiresHoldsAndReturnsGaCapacity() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, 100, false, 0, 0);
        HoldView ga = reservations.createHold(user(7), show.showId(), new HoldRequest(null, show.gaTierId(), 5, null), null);
        assertThat(gaHeld(show.showId())).isEqualTo(5);

        clock.advance(Duration.ofMinutes(8)); // movie TTL is 7 min
        while (reservations.expireBatch(500) == 500) {
            // drain
        }

        assertThat(gaHeld(show.showId())).isZero();
        assertThat(reservations.getHold(ga.holdId(), user(7)).status()).isEqualTo("EXPIRED");
    }

    /** Spec §5.4 safety rule: sweeping hold A must not free a seat that B already took lazily. */
    @Test
    void sweepingAnExpiredHoldNeverFreesASeatSomeoneElseTook() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        long seat = show.seat(0, 5, 10);
        HoldView a = reservations.createHold(user(8), show.showId(), new HoldRequest(List.of(seat), null, null, null), null);
        clock.advance(Duration.ofMinutes(8));

        HoldView b = reservations.createHold(user(9), show.showId(), new HoldRequest(List.of(seat), null, null, null), null);
        while (reservations.expireBatch(500) == 500) {
            // drain
        }

        assertThat(reservations.getHold(a.holdId(), user(8)).status()).isEqualTo("EXPIRED");
        UUID owner = jdbc.sql("SELECT hold_id FROM show_seats WHERE show_id = :s AND venue_seat_id = :id")
                .param("s", show.showId()).param("id", seat).query(UUID.class).single();
        assertThat(owner).isEqualTo(b.holdId());
        assertThat(statusOf(show.showId(), seat)).isEqualTo("HELD");
    }

    private String statusOf(long showId, long seat) {
        return jdbc.sql("SELECT status FROM show_seats WHERE show_id = :s AND venue_seat_id = :id")
                .param("s", showId).param("id", seat).query(String.class).single();
    }

    private int gaHeld(long showId) {
        return jdbc.sql("SELECT held FROM ga_inventory WHERE show_id = :s").param("s", showId).query(Integer.class).single();
    }
}
