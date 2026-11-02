package com.ticketforge.realtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.ticketforge.IntegrationTest;
import com.ticketforge.catalog.ShowCategory;
import com.ticketforge.reservation.HoldViews.HoldRequest;
import com.ticketforge.reservation.ReservationService;

class SeatStreamIntegrationTests extends IntegrationTest {

    @Autowired
    ReservationService reservations;

    @Test
    void viewersSeeHoldsReleasesAndGaCountsLive() throws Exception {
        ensureUsers(3);
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, 100, false, 0, 0);
        BlockingQueue<String> events = subscribe("/shows/" + show.showId() + "/seats/stream");
        assertThat(events.poll(5, TimeUnit.SECONDS)).startsWith("hello|");

        long seat = show.seat(0, 5, 10);
        var hold = reservations.createHold(user(1), show.showId(), new HoldRequest(List.of(seat), null, null, null), null);
        assertThat(events.poll(5, TimeUnit.SECONDS)).startsWith("seats|").contains("\"HELD\"").contains(String.valueOf(seat));

        reservations.release(hold.holdId(), user(1));
        assertThat(events.poll(5, TimeUnit.SECONDS)).startsWith("seats|").contains("\"RELEASED\"");

        reservations.createHold(user(2), show.showId(), new HoldRequest(null, show.gaTierId(), 3, null), null);
        assertThat(events.poll(5, TimeUnit.SECONDS)).startsWith("ga|").contains("\"available\":97");
    }

    @Test
    void rolledBackHoldsAreNeverBroadcast() throws Exception {
        ensureUsers(3);
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        BlockingQueue<String> events = subscribe("/shows/" + show.showId() + "/seats/stream");
        assertThat(events.poll(5, TimeUnit.SECONDS)).startsWith("hello|");
        reservations.createHold(user(1), show.showId(), new HoldRequest(List.of(show.seat(0, 5, 10)), null, null, null), null);
        assertThat(events.poll(5, TimeUnit.SECONDS)).startsWith("seats|");

        // seat 5 is taken, so this claim of 4+5 rolls back after holding 4 inside its transaction
        try {
            reservations.createHold(user(2), show.showId(),
                    new HoldRequest(List.of(show.seat(0, 4, 10), show.seat(0, 5, 10)), null, null, null), null);
        } catch (RuntimeException expected) {
            // SEATS_UNAVAILABLE
        }
        assertThat(events.poll(1, TimeUnit.SECONDS)).isNull();
    }

    private BlockingQueue<String> subscribe(String path) {
        BlockingQueue<String> events = new LinkedBlockingQueue<>();
        Thread.ofVirtual().start(() -> {
            try {
                HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                        .timeout(Duration.ofSeconds(30)).GET().build();
                HttpResponse<java.io.InputStream> resp = HttpClient.newHttpClient()
                        .send(req, HttpResponse.BodyHandlers.ofInputStream());
                try (BufferedReader in = new BufferedReader(new InputStreamReader(resp.body(), StandardCharsets.UTF_8))) {
                    String name = null;
                    String line;
                    while ((line = in.readLine()) != null) {
                        if (line.startsWith("event:")) {
                            name = line.substring(6).trim();
                        } else if (line.startsWith("data:") && name != null) {
                            events.add(name + "|" + line.substring(5).trim());
                            name = null;
                        }
                    }
                }
            } catch (Exception ignored) {
                // test ended
            }
        });
        return events;
    }
}
