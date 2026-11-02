package com.ticketforge.verification;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.ticketforge.IntegrationTest;
import com.ticketforge.catalog.ShowCategory;
import com.ticketforge.reservation.HoldViews.HoldRequest;
import com.ticketforge.reservation.ReservationService;

/** The checker must actually catch corruption, or a green dashboard means nothing. */
class InvariantCheckerTests extends IntegrationTest {

    @Autowired
    InvariantChecker checker;

    @Autowired
    ReservationService reservations;

    @Test
    void detectsABookedSeatWithoutAConfirmedHoldAndAGaCounterDrift() {
        ensureUsers(1);
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, 50, false, 0, 0);
        long seat = show.seat(0, 5, 10);
        reservations.createHold(user(1), show.showId(), new HoldRequest(List.of(seat), null, null, null), null);

        jdbc.sql("UPDATE show_seats SET status = 'BOOKED', held_until = NULL WHERE show_id = :s AND venue_seat_id = :id")
                .param("s", show.showId()).param("id", seat).update();
        jdbc.sql("UPDATE ga_inventory SET held = held + 1 WHERE show_id = :s").param("s", show.showId()).update();
        try {
            assertThat(checker.run())
                    .containsEntry("booked_seat_without_confirmed_hold", 1L)
                    .containsEntry("ga_counter_mismatch", 1L);
        } finally {
            jdbc.sql("UPDATE show_seats SET status = 'HELD', held_until = now() + interval '1 hour' WHERE show_id = :s AND venue_seat_id = :id")
                    .param("s", show.showId()).param("id", seat).update();
            jdbc.sql("UPDATE ga_inventory SET held = held - 1 WHERE show_id = :s").param("s", show.showId()).update();
        }
    }
}
