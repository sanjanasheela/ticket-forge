package com.ticketforge;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.awaitility.Awaitility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.jayway.jsonpath.JsonPath;
import com.ticketforge.contracts.SagaMessages.HoldInfo;
import com.ticketforge.security.TokenService;

/**
 * Base for tests against the whole booking-service, a real Postgres and a real Kafka. reservation-service is a
 * {@link StubReservation}: tests decide what it says about each hold and how it answers ConfirmHold.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "ticketforge.saga.reply-wait=2s")
@AutoConfigureMockMvc
@Import(TestcontainersConfig.class)
public abstract class IntegrationTest {

    @Autowired
    protected MockMvcTester mvc;
    @Autowired
    protected JdbcClient jdbc;
    @Autowired
    protected TestClock clock;
    @Autowired
    protected TokenService tokens;
    @Autowired
    protected StubReservation reservation;

    /** A buyer: id plus a JWT for it. */
    protected record Buyer(UUID id, String token) {
    }

    protected Buyer newBuyer() {
        UUID id = UUID.randomUUID();
        return new Buyer(id, tokens.issueAccess(id, "USER"));
    }

    protected String adminToken() {
        return tokens.issueAccess(UUID.randomUUID(), "ADMIN");
    }

    /** A live hold of {@code seats} seats at 250.00 each, as reservation-service would report it. */
    protected UUID liveHold(int seats) {
        return hold(seats, Instant.now().plus(Duration.ofMinutes(7)));
    }

    protected UUID hold(int seats, Instant expiresAt) {
        UUID id = UUID.randomUUID();
        List<HoldInfo.Item> items = new java.util.ArrayList<>();
        for (int i = 1; i <= seats; i++) {
            items.add(new HoldInfo.Item((long) i, 1, "MAIN A-" + i, 1, 25_000));
        }
        reservation.registerHold(new HoldInfo(id, 42, 25_000L * seats, "INR", expiresAt, items));
        return id;
    }

    protected MvcTestResult checkout(String token, UUID holdId, String key) {
        var req = mvc.post().uri("/holds/" + holdId + "/checkout").header("Authorization", "Bearer " + token);
        if (key != null) {
            req = req.header("Idempotency-Key", key);
        }
        return req.exchange();
    }

    /** The mock gateway's behaviour: latency, decline rate, and the rate of lost responses (charge happens, answer is lost). */
    protected void gateway(long latencyMs, double failureRate, double timeoutRate) {
        assertThat(mvc.put().uri("/admin/payment-gateway/config").header("Authorization", "Bearer " + adminToken())
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"latencyMs\":%d,\"jitterMs\":0,\"failureRate\":%s,\"timeoutRate\":%s}"
                        .formatted(latencyMs, failureRate, timeoutRate)).exchange())
                .hasStatusOk();
    }

    protected String sagaState(UUID holdId) {
        return jdbc.sql("SELECT state FROM sagas WHERE hold_id = :h").param("h", holdId).query(String.class).optional().orElse("NONE");
    }

    protected void awaitSagaState(UUID holdId, String expected) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(sagaState(holdId)).isEqualTo(expected));
    }

    protected String paymentStatus(UUID holdId) {
        return jdbc.sql("SELECT status FROM payments WHERE hold_id = :h ORDER BY created_at DESC LIMIT 1")
                .param("h", holdId).query(String.class).optional().orElse("NONE");
    }

    protected long count(String sql, Object... params) {
        var s = jdbc.sql(sql);
        for (int i = 0; i < params.length; i++) {
            s = s.param(params[i]);
        }
        return s.query(Long.class).single();
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
