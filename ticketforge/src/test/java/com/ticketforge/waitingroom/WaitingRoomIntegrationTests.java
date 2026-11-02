package com.ticketforge.waitingroom;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.ticketforge.IntegrationTest;
import com.ticketforge.catalog.ShowCategory;
import com.ticketforge.shared.ApiException;

class WaitingRoomIntegrationTests extends IntegrationTest {

    @Autowired
    WaitingRoomService queue;

    @Test
    void admitsStrictlyInJoinOrderAndTheTokenUnlocksHolds() {
        TestShow show = createShow(ShowCategory.CONCERT, 1, 10, null, true, null, 0);
        jdbc.sql("UPDATE shows SET admit_rate_per_sec = 2 WHERE id = :s").param("s", show.showId()).update();

        List<String> tokens = new ArrayList<>();
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            String t = newUser();
            tokens.add(t);
            MvcTestResult joined = postJson("/shows/" + show.showId() + "/queue", t, "");
            assertThat(joined).hasStatusOk();
            positions.add(((Number) read(joined, "$.position")).longValue());
        }
        assertThat(positions).isSorted();
        // joining again keeps your place
        assertThat(((Number) read(postJson("/shows/" + show.showId() + "/queue", tokens.get(3), ""), "$.position")).longValue())
                .isEqualTo(positions.get(3));

        queue.tick();

        List<Boolean> admitted = tokens.stream()
                .map(t -> (Boolean) read(get("/shows/" + show.showId() + "/queue", t), "$.admitted")).toList();
        int k = admitted.indexOf(false);
        assertThat(k).as("at least one tick's worth admitted").isGreaterThanOrEqualTo(2);
        assertThat(admitted.subList(k, admitted.size())).as("admitted users form a prefix of the join order")
                .containsOnly(false);

        // the last user is still waiting
        assertThat(get("/shows/" + show.showId() + "/queue/access", tokens.get(5)))
                .hasStatus(409).bodyJson().extractingPath("$.code").isEqualTo("NOT_ADMITTED_YET");

        // the first one gets a token, and only with it can hold seats
        String access = read(get("/shows/" + show.showId() + "/queue/access", tokens.get(0)), "$.token");
        String body = "{\"seatIds\":[%d,%d]}".formatted(show.seat(0, 1, 10), show.seat(0, 2, 10));
        assertThat(postJson("/shows/" + show.showId() + "/holds", tokens.get(0), body))
                .hasStatus(403).bodyJson().extractingPath("$.code").isEqualTo("QUEUE_REQUIRED");
        assertThat(mvc.post().uri("/shows/" + show.showId() + "/holds").header("Authorization", "Bearer " + tokens.get(0))
                .header("X-Queue-Token", access).contentType("application/json").content(body))
                .hasStatus(201);
    }

    @Test
    void showWithoutWaitingRoomRejectsJoining() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 4, null, false, 0, 0);
        try {
            queue.join(show.showId(), user(1));
        } catch (ApiException e) {
            assertThat(e.code()).isEqualTo("QUEUE_NOT_ENABLED");
            return;
        }
        throw new AssertionError("expected QUEUE_NOT_ENABLED");
    }

    @Test
    void positionIsStreamedOverSse() throws Exception {
        TestShow show = createShow(ShowCategory.CONCERT, 1, 10, null, true, null, 0);
        // sale opens in an hour, so nobody is admitted while we watch
        jdbc.sql("UPDATE shows SET sale_opens_at = now() + interval '1 hour' WHERE id = :s").param("s", show.showId()).update();
        String token = newUser();
        assertThat(postJson("/shows/" + show.showId() + "/queue", token, "")).hasStatusOk();

        List<String> events = readSse("/shows/" + show.showId() + "/queue/stream?access_token=" + token, 1, Duration.ofSeconds(5));

        assertThat(events.getFirst()).startsWith("queue|").contains("\"admitted\":false").contains("\"position\"");
    }

    /** Reads the first {@code count} events as "name|data". */
    protected List<String> readSse(String path, int count, Duration timeout) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(timeout).GET().build();
        HttpResponse<java.io.InputStream> resp = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
        assertThat(resp.statusCode()).isEqualTo(200);
        List<String> events = new ArrayList<>();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(resp.body(), StandardCharsets.UTF_8))) {
            String name = null;
            String line;
            while (events.size() < count && (line = in.readLine()) != null) {
                if (line.startsWith("event:")) {
                    name = line.substring(6).trim();
                } else if (line.startsWith("data:") && name != null) {
                    events.add(name + "|" + line.substring(5).trim());
                    name = null;
                }
            }
        }
        return events;
    }
}
