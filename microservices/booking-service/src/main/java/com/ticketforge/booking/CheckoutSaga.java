package com.ticketforge.booking;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import com.ticketforge.booking.CheckoutResult.Outcome;
import com.ticketforge.booking.Saga.State;
import com.ticketforge.contracts.SagaMessages.BookingEvent;
import com.ticketforge.contracts.SagaMessages.ConfirmHold;
import com.ticketforge.contracts.SagaMessages.ConfirmHoldReply;
import com.ticketforge.contracts.SagaMessages.HoldInfo;
import com.ticketforge.contracts.Topics;
import com.ticketforge.messaging.Outbox;
import com.ticketforge.payment.PaymentGateway.ChargeResult;
import com.ticketforge.payment.PaymentGateway.GatewayTimeoutException;
import com.ticketforge.payment.PaymentService;
import com.ticketforge.shared.ApiException;
import com.ticketforge.shared.Sql;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import tools.jackson.databind.json.JsonMapper;

/**
 * The checkout saga, orchestrated here. It replaces Phase 1's single transaction
 * ("mark payment paid + confirm the hold + insert the booking") now that the hold lives in another
 * service's database.
 *
 * <pre>
 *   1 VERIFY   ask reservation-service: is the hold live, what does it cost?        (HTTP, fails closed)
 *   2 CHARGE   take the money, with no database transaction open                     (the one step that cannot be rolled back)
 *   3 CONFIRM  command reservation-service to confirm the hold                       (Kafka, via the outbox)
 *   4 BOOK     on "confirmed": write the booking.   On "not confirmed": REFUND (the compensation)
 * </pre>
 *
 * Each step's outcome is committed to the {@code sagas} table with a guarded state change, so a crash
 * anywhere resumes from the table ({@link SagaRecovery}, {@link PaymentReconciler}) and money is never lost.
 */
@Service
class CheckoutSaga {

    private static final Logger log = LoggerFactory.getLogger(CheckoutSaga.class);

    private final SagaRepository sagas;
    private final ReservationClient reservations;
    private final PaymentService payments;
    private final Outbox outbox;
    private final TransactionTemplate tx;
    private final JdbcClient jdbc;
    private final ReplyWaiters waiters;
    private final JsonMapper json;
    private final Clock clock;
    private final MeterRegistry meters;
    private final Duration replyWait;

    CheckoutSaga(SagaRepository sagas, ReservationClient reservations, PaymentService payments, Outbox outbox,
            TransactionTemplate tx, JdbcClient jdbc, ReplyWaiters waiters, JsonMapper json, Clock clock,
            MeterRegistry meters, @Value("${ticketforge.saga.reply-wait:5s}") Duration replyWait) {
        this.sagas = sagas;
        this.reservations = reservations;
        this.payments = payments;
        this.outbox = outbox;
        this.tx = tx;
        this.jdbc = jdbc;
        this.waiters = waiters;
        this.json = json;
        this.clock = clock;
        this.meters = meters;
        this.replyWait = replyWait;
    }

    // ===== entry point =====================================================================================

    CheckoutResult checkout(UUID userId, UUID holdId) {
        Timer.Sample sample = Timer.start(meters);
        String outcome = "error";
        try {
            CheckoutResult result = run(userId, holdId);
            outcome = result.outcome().name();
            return result;
        } catch (ApiException e) {
            outcome = e.code();
            throw e;
        } finally {
            sample.stop(meters.timer("ticketforge.checkout", "outcome", outcome));
        }
    }

    private CheckoutResult run(UUID userId, UUID holdId) {
        Optional<Saga> existing = sagas.findByHold(holdId);
        if (existing.isPresent()) {
            Saga saga = existing.get();
            if (!saga.userId().equals(userId)) {
                throw ApiException.notFound("HOLD_NOT_FOUND", "Hold not found");
            }
            switch (saga.state()) {
                case BOOKED, REFUNDED, CHARGE_UNKNOWN -> {
                    return resultOf(saga);
                }
                case FAILED -> {
                    // declined earlier: the hold is untouched, so the buyer may try again
                }
                default -> throw ApiException.conflict("CHECKOUT_IN_PROGRESS",
                        "This hold is already being paid for or has been paid");
            }
        }

        // step 1: VERIFY (before any money moves; failing here costs nothing)
        HoldInfo hold = reservations.verify(holdId, userId);

        // the saga row and the pending payment are created together, or not at all
        UUID[] ids = tx.execute(status -> {
            UUID sagaId = sagas.start(holdId, userId, hold.showId(), hold.totalMinor(), hold.currency(), json.writeValueAsString(hold.items()))
                    .orElseThrow(() -> ApiException.conflict("CHECKOUT_IN_PROGRESS", "This hold is already being paid for or has been paid"));
            UUID paymentId = payments.createPending(holdId, userId, hold.totalMinor(), hold.currency());
            sagas.attachPayment(sagaId, paymentId);
            return new UUID[] {sagaId, paymentId};
        });
        UUID sagaId = ids[0];
        UUID paymentId = ids[1];

        // step 2: CHARGE, outside any transaction: a slow processor must not pin a pooled connection or a row lock
        ChargeResult charge;
        try {
            charge = payments.gateway().charge(paymentId.toString(), hold.totalMinor(), hold.currency());
        } catch (GatewayTimeoutException e) {
            sagas.move(sagaId, List.of(State.CHARGING), State.CHARGE_UNKNOWN, null, "gateway_timeout");
            log.warn("Payment outcome unknown, reconciler will resolve payment_id={} hold_id={}", paymentId, holdId);
            return new CheckoutResult(Outcome.PAYMENT_PENDING, null, paymentId, "Payment is being confirmed");
        }
        if (!charge.succeeded()) {
            tx.executeWithoutResult(status -> {
                payments.markFailed(paymentId, charge.declineReason());
                sagas.move(sagaId, List.of(State.CHARGING), State.FAILED, null, charge.declineReason());
            });
            return new CheckoutResult(Outcome.PAYMENT_FAILED, null, paymentId,
                    "Payment declined; your seats stay held until the hold expires");
        }
        return moneyTaken(sagaId, paymentId, holdId, userId, charge.gatewayRef(), true);
    }

    // ===== step 3: the money is taken, ask reservation-service to confirm the hold ===========================

    /**
     * Also called by the reconciler for a charge whose response was lost.
     * @param wait whether to hold the caller until the saga settles (HTTP) or return at once (background)
     */
    CheckoutResult moneyTaken(UUID sagaId, UUID paymentId, UUID holdId, UUID userId, String gatewayRef, boolean wait) {
        CompletableFuture<Void> settled = waiters.register(sagaId);   // before the command goes out, or a fast reply could be missed
        Boolean advanced = tx.execute(status -> {
            if (!payments.markSucceeded(paymentId, gatewayRef)) {
                return false;   // resolved concurrently (checkout thread vs reconciler)
            }
            sagas.move(sagaId, List.of(State.CHARGING, State.CHARGE_UNKNOWN), State.CHARGED, gatewayRef, null);
            outbox.add(Topics.SAGA_RESERVATION_COMMANDS, holdId.toString(), Topics.CONFIRM_HOLD,
                    new ConfirmHold(sagaId, holdId, userId));
            return true;
        });
        if (Boolean.TRUE.equals(advanced) && wait) {
            waiters.await(settled, sagaId, replyWait);
        }
        return sagas.find(sagaId).map(this::resultOf)
                .orElse(new CheckoutResult(Outcome.PAYMENT_PENDING, null, paymentId, "Payment is being confirmed"));
    }

    // ===== step 4: reservation-service answered ==============================================================

    void onReply(ConfirmHoldReply reply) {
        Optional<Saga> refundNeeded = tx.execute(status -> {
            Optional<Saga> locked = sagas.lock(reply.sagaId());
            if (locked.isEmpty() || locked.get().state() != State.CHARGED) {
                return Optional.<Saga>empty();   // a duplicate or late reply: the saga has already moved on
            }
            Saga saga = locked.get();
            if (reply.confirmed()) {
                UUID bookingId = insertBooking(saga);
                sagas.markBooked(saga.id(), bookingId);
                outbox.add(Topics.BOOKING_EVENTS, bookingId.toString(), Topics.BOOKING_CONFIRMED, new BookingEvent(
                        bookingId, saga.holdId(), saga.userId(), saga.showId(), saga.totalMinor(), saga.currency()));
                log.info("Booking confirmed booking_id={} hold_id={} payment_id={}", bookingId, saga.holdId(), saga.paymentId());
                return Optional.<Saga>empty();
            }
            sagas.move(saga.id(), List.of(State.CHARGED), State.REFUNDING, null, reply.reason());
            return Optional.of(saga);
        });
        if (refundNeeded != null && refundNeeded.isPresent()) {
            refund(refundNeeded.get());   // the compensation: money taken but the seats cannot be given
        }
        waiters.complete(reply.sagaId());
    }

    /** Compensation: give the money back. Safe to repeat (gateway refund is idempotent, state changes are guarded). */
    void refund(Saga saga) {
        payments.gateway().refund(saga.gatewayRef());
        tx.executeWithoutResult(status -> {
            payments.markRefunded(saga.paymentId(), "hold_unavailable");
            if (sagas.move(saga.id(), List.of(State.REFUNDING), State.REFUNDED, null, null)) {
                outbox.add(Topics.BOOKING_EVENTS, saga.holdId().toString(), Topics.BOOKING_REFUNDED, new BookingEvent(
                        null, saga.holdId(), saga.userId(), saga.showId(), saga.totalMinor(), saga.currency()));
            }
        });
        meters.counter("ticketforge.late.payment.refunds").increment();
        log.warn("Payment refunded payment_id={} hold_id={} reason={}", saga.paymentId(), saga.holdId(), saga.reason());
        waiters.complete(saga.id());
    }

    /** Resends ConfirmHold for a saga whose reply never arrived. Harmless if the first one was in fact handled. */
    void resendConfirm(Saga saga) {
        tx.executeWithoutResult(status -> {
            sagas.touch(saga.id());
            outbox.add(Topics.SAGA_RESERVATION_COMMANDS, saga.holdId().toString(), Topics.CONFIRM_HOLD,
                    new ConfirmHold(saga.id(), saga.holdId(), saga.userId()));
        });
        log.warn("ConfirmHold resent saga_id={} hold_id={}", saga.id(), saga.holdId());
    }

    // ===== helpers ===========================================================================================

    private UUID insertBooking(Saga saga) {
        UUID id = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO bookings (id, hold_id, user_id, show_id, payment_id, status, total_minor, currency, items, created_at)
                        VALUES (:id, :h, :u, :s, :p, 'CONFIRMED', :t, :c, :items, :now)""")
                .param("id", id).param("h", saga.holdId()).param("u", saga.userId()).param("s", saga.showId())
                .param("p", saga.paymentId()).param("t", saga.totalMinor()).param("c", saga.currency())
                .param("items", saga.items()).param("now", Sql.ts(clock.instant()))
                .update();
        return id;
    }

    CheckoutResult resultOf(Saga saga) {
        return switch (saga.state()) {
            case BOOKED -> new CheckoutResult(Outcome.CONFIRMED, saga.bookingId(), saga.paymentId(), "Booked");
            case REFUNDED -> new CheckoutResult(Outcome.HOLD_EXPIRED_REFUNDED, null, saga.paymentId(),
                    "Your hold expired before the payment completed; the charge has been refunded");
            case FAILED -> new CheckoutResult(Outcome.PAYMENT_FAILED, null, saga.paymentId(),
                    "Payment declined; your seats stay held until the hold expires");
            case CHARGING, CHARGE_UNKNOWN, CHARGED, REFUNDING ->
                    new CheckoutResult(Outcome.PAYMENT_PENDING, null, saga.paymentId(), "Payment is being confirmed");
        };
    }

}
