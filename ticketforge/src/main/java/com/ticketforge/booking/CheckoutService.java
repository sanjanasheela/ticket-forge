package com.ticketforge.booking;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import com.ticketforge.payment.PaymentGateway.ChargeResult;
import com.ticketforge.payment.PaymentGateway.GatewayTimeoutException;
import com.ticketforge.payment.PaymentService;
import com.ticketforge.reservation.HoldNoLongerValidException;
import com.ticketforge.reservation.HoldViews.ActiveHold;
import com.ticketforge.reservation.ReservationService;
import com.ticketforge.shared.ApiException;
import com.ticketforge.shared.Sql;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Hold -> Pay -> Confirm (spec §5.3). No database transaction is open while the gateway is called:
 * a slow processor must not pin a pooled connection or a row lock.
 */
@Service
class CheckoutService {

    private static final Logger log = LoggerFactory.getLogger(CheckoutService.class);

    enum Outcome { CONFIRMED, PAYMENT_PENDING, PAYMENT_FAILED, HOLD_EXPIRED_REFUNDED }

    record CheckoutResult(Outcome outcome, UUID bookingId, UUID paymentId, String message) {
    }

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final ReservationService reservations;
    private final PaymentService payments;
    private final Clock clock;
    private final MeterRegistry meters;

    CheckoutService(JdbcClient jdbc, TransactionTemplate tx, ReservationService reservations, PaymentService payments,
            Clock clock, MeterRegistry meters) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.reservations = reservations;
        this.payments = payments;
        this.clock = clock;
        this.meters = meters;
    }

    CheckoutResult checkout(UUID userId, UUID holdId) {
        Timer.Sample sample = Timer.start(meters);
        String outcome = "error";
        try {
            CheckoutResult result = doCheckout(userId, holdId);
            outcome = result.outcome().name();
            return result;
        } catch (ApiException e) {
            outcome = e.code();
            throw e;
        } finally {
            sample.stop(meters.timer("ticketforge.checkout", "outcome", outcome));
        }
    }

    private CheckoutResult doCheckout(UUID userId, UUID holdId) {
        Optional<UUID> existing = bookingFor(holdId, userId);
        if (existing.isPresent()) {
            return new CheckoutResult(Outcome.CONFIRMED, existing.get(), null, "Already booked");
        }

        // tx1: the hold must be live at the moment we start charging
        Instant now = clock.instant();
        ActiveHold hold = reservations.findActive(holdId, userId, now).orElseThrow(() -> reservations.exists(holdId, userId)
                ? new ApiException(HttpStatus.CONFLICT, "HOLD_EXPIRED", "This hold has expired or was released")
                : ApiException.notFound("HOLD_NOT_FOUND", "Hold not found"));
        UUID paymentId = payments.createPending(holdId, userId, hold.totalMinor(), hold.currency());

        // outside any transaction
        ChargeResult charge;
        try {
            charge = payments.gateway().charge(paymentId.toString(), hold.totalMinor(), hold.currency());
        } catch (GatewayTimeoutException e) {
            log.warn("Payment outcome unknown, reconciler will resolve payment_id={} hold_id={}", paymentId, holdId);
            return new CheckoutResult(Outcome.PAYMENT_PENDING, null, paymentId, "Payment is being confirmed");
        }
        if (!charge.succeeded()) {
            payments.markFailed(paymentId, charge.declineReason());
            return new CheckoutResult(Outcome.PAYMENT_FAILED, null, paymentId,
                    "Payment declined; your seats stay held until the hold expires");
        }
        return finalizePaid(paymentId, holdId, userId, charge.gatewayRef());
    }

    /**
     * Money has been taken. Either the hold is still ours and becomes a booking, or it expired
     * meanwhile and the charge is refunded (local compensation; the Saga's job in Phase 2).
     * Also called by the reconciler, which is why the first step is a guarded status change.
     */
    CheckoutResult finalizePaid(UUID paymentId, UUID holdId, UUID userId, String gatewayRef) {
        try {
            UUID bookingId = tx.execute(status -> {
                if (!payments.markSucceeded(paymentId, gatewayRef)) {
                    return null; // resolved concurrently (checkout thread vs reconciler)
                }
                Instant now = clock.instant();
                reservations.confirm(holdId, userId, now);
                return insertBooking(paymentId, holdId, userId, now);
            });
            if (bookingId == null) {
                return bookingFor(holdId, userId)
                        .map(id -> new CheckoutResult(Outcome.CONFIRMED, id, paymentId, "Booked"))
                        .orElse(new CheckoutResult(Outcome.PAYMENT_PENDING, null, paymentId, "Payment is being confirmed"));
            }
            log.info("Booking confirmed booking_id={} hold_id={} payment_id={}", bookingId, holdId, paymentId);
            return new CheckoutResult(Outcome.CONFIRMED, bookingId, paymentId, "Booked");
        } catch (HoldNoLongerValidException e) {
            refundLatePayment(paymentId, gatewayRef, e.getMessage());
            return new CheckoutResult(Outcome.HOLD_EXPIRED_REFUNDED, null, paymentId,
                    "Your hold expired before the payment completed; the charge has been refunded");
        }
    }

    private void refundLatePayment(UUID paymentId, String gatewayRef, String reason) {
        tx.executeWithoutResult(status -> {
            payments.markSucceeded(paymentId, gatewayRef);
            payments.markRefunded(paymentId, "hold_expired");
        });
        payments.gateway().refund(gatewayRef);
        meters.counter("ticketforge.late.payment.refunds").increment();
        log.warn("Late payment refunded payment_id={} reason={}", paymentId, reason);
    }

    private UUID insertBooking(UUID paymentId, UUID holdId, UUID userId, Instant now) {
        UUID id = UUID.randomUUID();
        jdbc.sql("""
                        INSERT INTO bookings (id, hold_id, user_id, show_id, payment_id, status, total_minor, currency, created_at)
                        SELECT :id, h.id, h.user_id, h.show_id, :p, 'CONFIRMED', h.total_minor, h.currency, :now
                        FROM holds h WHERE h.id = :h""")
                .param("id", id).param("p", paymentId).param("now", Sql.ts(now)).param("h", holdId)
                .update();
        return id;
    }

    private Optional<UUID> bookingFor(UUID holdId, UUID userId) {
        return jdbc.sql("SELECT id FROM bookings WHERE hold_id = :h AND user_id = :u AND status = 'CONFIRMED'")
                .param("h", holdId).param("u", userId).query(UUID.class).optional();
    }
}
