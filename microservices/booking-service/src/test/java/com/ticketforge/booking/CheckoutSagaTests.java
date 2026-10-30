package com.ticketforge.booking;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.ticketforge.IntegrationTest;
import com.ticketforge.StubReservation.OnConfirm;
import com.ticketforge.contracts.SagaMessages.ConfirmHoldReply;

/**
 * The Phase 1 checkout behaviours (same statuses, same guarantees) now delivered by the saga, plus the
 * failure modes that only exist once the hold lives in another service.
 */
class CheckoutSagaTests extends IntegrationTest {

    @Autowired
    PaymentReconciler reconciler;
    @Autowired
    SagaRecovery recovery;

    @BeforeEach
    void fastReliableGateway() {
        gateway(0, 0, 0);
        reservation.setDown(false);
    }

    @AfterEach
    void restore() {
        gateway(150, 0.05, 0);
        reservation.setDown(false);
    }

    // ---- the Phase 1 behaviours, ported ---------------------------------------------------------

    @Test
    void holdPayConfirmBooksTheSeats() {
        Buyer b = newBuyer();
        UUID hold = liveHold(2);

        MvcTestResult r = checkout(b.token(), hold, "key-" + UUID.randomUUID());

        assertThat(r).hasStatus(201).bodyJson().extractingPath("$.outcome").isEqualTo("CONFIRMED");
        assertThat(sagaState(hold)).isEqualTo("BOOKED");
        assertThat(paymentStatus(hold)).isEqualTo("SUCCEEDED");
        assertThat(mvc.get().uri("/me/bookings").header("Authorization", "Bearer " + b.token()).exchange())
                .hasStatusOk().bodyJson().extractingPath("$[0].items.length()").isEqualTo(2);
    }

    @Test
    void checkoutRequiresAnIdempotencyKey() {
        Buyer b = newBuyer();
        assertThat(checkout(b.token(), liveHold(1), null))
                .hasStatus(400).bodyJson().extractingPath("$.code").isEqualTo("IDEMPOTENCY_KEY_REQUIRED");
    }

    @Test
    void retryWithTheSameKeyReplaysAndChargesOnce() {
        Buyer b = newBuyer();
        UUID hold = liveHold(1);
        String key = "key-" + UUID.randomUUID();

        MvcTestResult first = checkout(b.token(), hold, key);
        MvcTestResult second = checkout(b.token(), hold, key);

        assertThat(second).hasStatus(201).hasHeader("Idempotent-Replayed", "true");
        assertThat(body(second)).isEqualTo(body(first));
        assertThat(count("SELECT count(*) FROM payments WHERE hold_id = ?", hold)).isEqualTo(1);
    }

    @Test
    void declinedPaymentKeepsTheHoldAndCanBeRetried() {
        Buyer b = newBuyer();
        UUID hold = liveHold(1);
        gateway(0, 1.0, 0);

        assertThat(checkout(b.token(), hold, "k1-" + UUID.randomUUID()))
                .hasStatus(402).bodyJson().extractingPath("$.outcome").isEqualTo("PAYMENT_FAILED");
        assertThat(sagaState(hold)).isEqualTo("FAILED");
        assertThat(reservation.commandsFor(hold)).as("nothing was sent to reservation: the hold is untouched").isZero();

        gateway(0, 0, 0);
        assertThat(checkout(b.token(), hold, "k2-" + UUID.randomUUID())).hasStatus(201);
        assertThat(sagaState(hold)).isEqualTo("BOOKED");
    }

    @Test
    void lostGatewayResponseIsResolvedByTheReconciler() {
        Buyer b = newBuyer();
        UUID hold = liveHold(1);
        gateway(0, 0, 1.0);   // the charge happens, the answer is lost

        assertThat(checkout(b.token(), hold, "key-" + UUID.randomUUID()))
                .hasStatus(202).bodyJson().extractingPath("$.outcome").isEqualTo("PAYMENT_PENDING");
        assertThat(sagaState(hold)).isEqualTo("CHARGE_UNKNOWN");

        gateway(0, 0, 0);
        clock.advance(Duration.ofSeconds(11));
        reconciler.reconcile();

        awaitSagaState(hold, "BOOKED");
        assertThat(paymentStatus(hold)).isEqualTo("SUCCEEDED");
    }

    /** The edge case the saga exists for: money taken, but reservation says the hold is gone. Compensate with a refund. */
    @Test
    void paymentThatLandsAfterExpiryIsRefunded() {
        Buyer b = newBuyer();
        UUID hold = liveHold(1);
        reservation.onConfirm(hold, OnConfirm.REJECT);

        MvcTestResult r = checkout(b.token(), hold, "key-" + UUID.randomUUID());

        assertThat(r).hasStatus(409).bodyJson().extractingPath("$.outcome").isEqualTo("HOLD_EXPIRED_REFUNDED");
        assertThat(sagaState(hold)).isEqualTo("REFUNDED");
        assertThat(paymentStatus(hold)).isEqualTo("REFUNDED");
        assertThat(count("SELECT count(*) FROM bookings WHERE hold_id = ?", hold)).isZero();
    }

    @Test
    void twoTabsCheckingOutTheSameHoldChargeOnce() throws Exception {
        Buyer b = newBuyer();
        UUID hold = liveHold(1);
        gateway(50, 0, 0);
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<Integer>> results = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 10; i++) {
                results.add(pool.submit(() -> {
                    gate.await();
                    return checkout(b.token(), hold, "tab-" + UUID.randomUUID()).getResponse().getStatus();
                }));
            }
            gate.countDown();
            List<Integer> statuses = new ArrayList<>();
            for (Future<Integer> f : results) {
                statuses.add(f.get());
            }
            assertThat(statuses).contains(201).allMatch(s -> s == 201 || s == 409);
        }
        assertThat(count("SELECT count(*) FROM payments WHERE hold_id = ? AND status = 'SUCCEEDED'", hold)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM bookings WHERE hold_id = ?", hold)).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM sagas WHERE hold_id = ?", hold)).isEqualTo(1);
    }

    // ---- failure modes that only exist with two services ----------------------------------------

    @Test
    void anExpiredHoldIsRejectedBeforeAnyoneIsCharged() {
        Buyer b = newBuyer();
        UUID hold = hold(1, Instant.now().minusSeconds(5));

        assertThat(checkout(b.token(), hold, "key-" + UUID.randomUUID()))
                .hasStatus(409).bodyJson().extractingPath("$.code").isEqualTo("HOLD_EXPIRED");
        assertThat(count("SELECT count(*) FROM payments WHERE hold_id = ?", hold)).as("nothing charged").isZero();
        assertThat(sagaState(hold)).isEqualTo("NONE");
    }

    @Test
    void anUnknownHoldIs404() {
        assertThat(checkout(newBuyer().token(), UUID.randomUUID(), "key-" + UUID.randomUUID()))
                .hasStatus(404).bodyJson().extractingPath("$.code").isEqualTo("HOLD_NOT_FOUND");
    }

    @Test
    void whenReservationIsUnreachableCheckoutFailsClosedWithNoCharge() {
        Buyer b = newBuyer();
        UUID hold = liveHold(1);
        reservation.setDown(true);

        assertThat(checkout(b.token(), hold, "key-" + UUID.randomUUID()))
                .hasStatus(503).bodyJson().extractingPath("$.code").isEqualTo("RESERVATION_UNAVAILABLE");
        assertThat(count("SELECT count(*) FROM payments WHERE hold_id = ?", hold)).as("nothing charged").isZero();

        reservation.setDown(false);
        assertThat(checkout(b.token(), hold, "key-" + UUID.randomUUID())).as("works again once reservation is back").hasStatus(201);
    }

    @Test
    void aBuyerCannotCheckOutSomeoneElsesHold() {
        Buyer owner = newBuyer();
        UUID hold = liveHold(1);
        assertThat(checkout(owner.token(), hold, "key-" + UUID.randomUUID())).hasStatus(201);

        assertThat(checkout(newBuyer().token(), hold, "key-" + UUID.randomUUID())).hasStatus(404);
    }

    @Test
    void aLostReplyIsRecoveredByResendingTheCommand() {
        Buyer b = newBuyer();
        UUID hold = liveHold(1);
        reservation.onConfirm(hold, OnConfirm.SILENT);   // reservation never answers

        assertThat(checkout(b.token(), hold, "key-" + UUID.randomUUID()))
                .hasStatus(202).bodyJson().extractingPath("$.outcome").isEqualTo("PAYMENT_PENDING");
        assertThat(sagaState(hold)).isEqualTo("CHARGED");
        Awaitility.await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(reservation.commandsFor(hold)).isEqualTo(1));

        reservation.onConfirm(hold, OnConfirm.CONFIRM);  // it recovers
        clock.advance(Duration.ofSeconds(16));
        recovery.recover();

        awaitSagaState(hold, "BOOKED");
        assertThat(reservation.commandsFor(hold)).as("the command was sent again").isGreaterThanOrEqualTo(2);
    }

    @Test
    void aDuplicateReplyDoesNotBookTwiceOrRefundABookedCheckout() {
        Buyer b = newBuyer();
        UUID hold = liveHold(1);
        assertThat(checkout(b.token(), hold, "key-" + UUID.randomUUID())).hasStatus(201);
        UUID sagaId = jdbc.sql("SELECT id FROM sagas WHERE hold_id = :h").param("h", hold).query(UUID.class).single();

        reservation.reply(new ConfirmHoldReply(sagaId, hold, true, null));
        reservation.reply(new ConfirmHoldReply(sagaId, hold, false, "HOLD_EXPIRED"));   // even a contradictory late one

        // a marker saga proves the duplicates above, which were published first, have been consumed
        UUID marker = liveHold(1);
        assertThat(checkout(b.token(), marker, "key-" + UUID.randomUUID())).hasStatus(201);
        assertThat(sagaState(hold)).isEqualTo("BOOKED");
        assertThat(paymentStatus(hold)).isEqualTo("SUCCEEDED");
        assertThat(count("SELECT count(*) FROM bookings WHERE hold_id = ?", hold)).isEqualTo(1);
    }

    @Test
    void aRefundInterruptedByACrashIsFinishedByRecovery() {
        Buyer b = newBuyer();
        UUID hold = liveHold(1);
        reservation.onConfirm(hold, OnConfirm.SILENT);
        checkout(b.token(), hold, "key-" + UUID.randomUUID());
        assertThat(sagaState(hold)).isEqualTo("CHARGED");
        // the process "died" after deciding to refund but before refunding
        jdbc.sql("UPDATE sagas SET state = 'REFUNDING', reason = 'HOLD_EXPIRED' WHERE hold_id = :h").param("h", hold).update();

        clock.advance(Duration.ofSeconds(11));
        recovery.recover();

        awaitSagaState(hold, "REFUNDED");
        assertThat(paymentStatus(hold)).isEqualTo("REFUNDED");
        assertThat(count("SELECT count(*) FROM bookings WHERE hold_id = ?", hold)).isZero();
    }

    @Test
    void everyStateChangeThatMatteredLeftAnEventForOthers() {
        Buyer b = newBuyer();
        UUID booked = liveHold(1);
        UUID refunded = liveHold(1);
        reservation.onConfirm(refunded, OnConfirm.REJECT);

        checkout(b.token(), booked, "key-" + UUID.randomUUID());
        checkout(b.token(), refunded, "key-" + UUID.randomUUID());

        assertThat(count("SELECT count(*) FROM outbox WHERE topic = 'booking.events' AND type = 'BookingConfirmed' AND payload LIKE ?",
                "%" + booked + "%")).isEqualTo(1);
        assertThat(count("SELECT count(*) FROM outbox WHERE topic = 'booking.events' AND type = 'BookingRefunded' AND payload LIKE ?",
                "%" + refunded + "%")).isEqualTo(1);
    }
}
