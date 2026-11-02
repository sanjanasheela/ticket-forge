package com.ticketforge.booking;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.ticketforge.IntegrationTest;
import com.ticketforge.catalog.ShowCategory;
import com.ticketforge.payment.PaymentService;

class CheckoutIntegrationTests extends IntegrationTest {

    @Autowired
    CheckoutService checkout;

    @Autowired
    PaymentReconciler reconciler;

    @Autowired
    PaymentService payments;

    @BeforeEach
    void fastReliableGateway() {
        gateway(0, 0, 0);
    }

    @AfterEach
    void restoreGateway() {
        gateway(150, 0.05, 0);
    }

    @Test
    void holdPayConfirmBooksTheSeats() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        String token = newUser();
        String holdId = hold(token, show, show.seat(0, 4, 10), show.seat(0, 5, 10));

        MvcTestResult r = checkout(token, holdId, "key-" + UUID.randomUUID());

        assertThat(r).hasStatus(201).bodyJson().extractingPath("$.outcome").isEqualTo("CONFIRMED");
        assertThat(seatStatuses(show.showId(), holdId)).containsOnly("BOOKED").hasSize(2);
        assertThat(get("/holds/" + holdId, token)).bodyJson().extractingPath("$.status").isEqualTo("CONFIRMED");
        assertThat(get("/me/bookings", token)).hasStatusOk().bodyJson().extractingPath("$[0].items.length()").isEqualTo(2);
    }

    @Test
    void checkoutRequiresAnIdempotencyKey() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        String token = newUser();
        String holdId = hold(token, show, show.seat(0, 4, 10));
        assertThat(mvc.post().uri("/holds/" + holdId + "/checkout").header("Authorization", "Bearer " + token))
                .hasStatus(400).bodyJson().extractingPath("$.code").isEqualTo("IDEMPOTENCY_KEY_REQUIRED");
    }

    @Test
    void retryWithTheSameKeyReplaysAndChargesOnce() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        String token = newUser();
        String holdId = hold(token, show, show.seat(0, 4, 10));
        String key = "key-" + UUID.randomUUID();

        MvcTestResult first = checkout(token, holdId, key);
        MvcTestResult second = checkout(token, holdId, key);

        assertThat(second).hasStatus(201).hasHeader("Idempotent-Replayed", "true");
        assertThat(body(second)).isEqualTo(body(first));
        assertThat(jdbc.sql("SELECT count(*) FROM payments WHERE hold_id = :h").param("h", UUID.fromString(holdId))
                .query(Integer.class).single()).isEqualTo(1);
    }

    @Test
    void reusingAKeyForADifferentRequestIsRejected() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        String token = newUser();
        String key = "key-" + UUID.randomUUID();
        assertThat(holdWithKey(token, show, key, show.seat(0, 1, 10), show.seat(0, 2, 10))).hasStatus(201);
        assertThat(holdWithKey(token, show, key, show.seat(0, 7, 10), show.seat(0, 8, 10)))
                .hasStatus(422).bodyJson().extractingPath("$.code").isEqualTo("IDEMPOTENCY_KEY_REUSED");
    }

    @Test
    void retriedHoldRequestReturnsTheSameHold() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        String token = newUser();
        String key = "key-" + UUID.randomUUID();
        String a = read(holdWithKey(token, show, key, show.seat(0, 4, 10)), "$.holdId");
        String b = read(holdWithKey(token, show, key, show.seat(0, 4, 10)), "$.holdId");
        assertThat(b).isEqualTo(a);
    }

    @Test
    void declinedPaymentKeepsTheHoldAndCanBeRetried() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        String token = newUser();
        String holdId = hold(token, show, show.seat(0, 4, 10));

        gateway(0, 1.0, 0);
        assertThat(checkout(token, holdId, "k1-" + UUID.randomUUID()))
                .hasStatus(402).bodyJson().extractingPath("$.outcome").isEqualTo("PAYMENT_FAILED");
        assertThat(seatStatuses(show.showId(), holdId)).containsOnly("HELD");

        gateway(0, 0, 0);
        assertThat(checkout(token, holdId, "k2-" + UUID.randomUUID())).hasStatus(201);
    }

    @Test
    void lostGatewayResponseIsResolvedByTheReconciler() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        String token = newUser();
        String holdId = hold(token, show, show.seat(0, 4, 10));

        gateway(0, 0, 1.0); // charge happens, response is lost
        assertThat(checkout(token, holdId, "key-" + UUID.randomUUID()))
                .hasStatus(202).bodyJson().extractingPath("$.outcome").isEqualTo("PAYMENT_PENDING");
        gateway(0, 0, 0);

        clock.advance(Duration.ofSeconds(11));
        reconciler.reconcile();

        assertThat(seatStatuses(show.showId(), holdId)).containsOnly("BOOKED");
        assertThat(paymentStatus(holdId)).isEqualTo("SUCCEEDED");
    }

    /** The edge case the Phase 2 Saga takes over: money taken just after the hold expired. */
    @Test
    void paymentThatLandsAfterExpiryIsRefunded() {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        String token = newUser();
        UUID userId = UUID.fromString(com.jayway.jsonpath.JsonPath.read(
                new String(java.util.Base64.getUrlDecoder().decode(token.split("\\.")[1])), "$.sub"));
        UUID holdId = UUID.fromString(hold(token, show, show.seat(0, 4, 10)));
        UUID paymentId = payments.createPending(holdId, userId, 25_000, "INR");

        clock.advance(Duration.ofMinutes(8)); // past the 7-minute movie hold
        var result = checkout.finalizePaid(paymentId, holdId, userId, "ch_late");

        assertThat(result.outcome()).isEqualTo(CheckoutService.Outcome.HOLD_EXPIRED_REFUNDED);
        assertThat(paymentStatus(holdId.toString())).isEqualTo("REFUNDED");
        assertThat(jdbc.sql("SELECT count(*) FROM bookings WHERE hold_id = :h").param("h", holdId)
                .query(Integer.class).single()).isZero();
    }

    @Test
    void twoTabsCheckingOutTheSameHoldChargeOnce() throws Exception {
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        String token = newUser();
        String holdId = hold(token, show, show.seat(0, 4, 10));
        gateway(50, 0, 0);

        CountDownLatch gate = new CountDownLatch(1);
        List<Future<Integer>> results = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 10; i++) {
                results.add(pool.submit(() -> {
                    gate.await();
                    return checkout(token, holdId, "tab-" + UUID.randomUUID()).getResponse().getStatus();
                }));
            }
            gate.countDown();
            List<Integer> statuses = new ArrayList<>();
            for (Future<Integer> f : results) {
                statuses.add(f.get());
            }
            assertThat(statuses).contains(201).allMatch(s -> s == 201 || s == 409);
        }
        assertThat(jdbc.sql("SELECT count(*) FROM payments WHERE hold_id = :h AND status = 'SUCCEEDED'")
                .param("h", UUID.fromString(holdId)).query(Integer.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM bookings WHERE hold_id = :h").param("h", UUID.fromString(holdId))
                .query(Integer.class).single()).isEqualTo(1);
    }

    // ---- helpers -----------------------------------------------------------------------------

    private void gateway(long latencyMs, double failureRate, double timeoutRate) {
        assertThat(mvc.put().uri("/admin/payment-gateway/config").header("Authorization", "Bearer " + adminToken())
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"latencyMs\":%d,\"jitterMs\":0,\"failureRate\":%s,\"timeoutRate\":%s}"
                        .formatted(latencyMs, failureRate, timeoutRate)))
                .hasStatusOk();
    }

    private String hold(String token, TestShow show, long... seats) {
        MvcTestResult r = holdWithKey(token, show, null, seats);
        assertThat(r).hasStatus(201);
        return read(r, "$.holdId");
    }

    private MvcTestResult holdWithKey(String token, TestShow show, String key, long... seats) {
        StringBuilder ids = new StringBuilder();
        for (long s : seats) {
            ids.append(ids.isEmpty() ? "" : ",").append(s);
        }
        var req = mvc.post().uri("/shows/" + show.showId() + "/holds").header("Authorization", "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON).content("{\"seatIds\":[" + ids + "]}");
        if (key != null) {
            req = req.header("Idempotency-Key", key);
        }
        return req.exchange();
    }

    private MvcTestResult checkout(String token, String holdId, String key) {
        return mvc.post().uri("/holds/" + holdId + "/checkout").header("Authorization", "Bearer " + token)
                .header("Idempotency-Key", key).exchange();
    }

    private List<String> seatStatuses(long showId, String holdId) {
        return jdbc.sql("SELECT status FROM show_seats WHERE show_id = :s AND hold_id = :h")
                .param("s", showId).param("h", UUID.fromString(holdId)).query(String.class).list();
    }

    private String paymentStatus(String holdId) {
        return jdbc.sql("SELECT status FROM payments WHERE hold_id = :h ORDER BY created_at DESC LIMIT 1")
                .param("h", UUID.fromString(holdId)).query(String.class).single();
    }
}
