package com.ticketforge.booking;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ticketforge.booking.CheckoutService.CheckoutResult;
import com.ticketforge.reservation.HoldViews.HoldItem;
import com.ticketforge.reservation.ReservationService;
import com.ticketforge.shared.CurrentUser;

@RestController
class BookingController {

    private final CheckoutService checkout;
    private final ReservationService reservations;
    private final JdbcClient jdbc;

    BookingController(CheckoutService checkout, ReservationService reservations, JdbcClient jdbc) {
        this.checkout = checkout;
        this.reservations = reservations;
        this.jdbc = jdbc;
    }

    /** Requires an Idempotency-Key header (enforced by the idempotency filter). */
    @PostMapping("/holds/{id}/checkout")
    ResponseEntity<CheckoutResult> checkout(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        CheckoutResult r = checkout.checkout(CurrentUser.id(jwt), id);
        HttpStatus status = switch (r.outcome()) {
            case CONFIRMED -> HttpStatus.CREATED;
            case PAYMENT_PENDING -> HttpStatus.ACCEPTED;
            case PAYMENT_FAILED -> HttpStatus.PAYMENT_REQUIRED;
            case HOLD_EXPIRED_REFUNDED -> HttpStatus.CONFLICT;
        };
        return ResponseEntity.status(status).body(r);
    }

    record BookingView(UUID id, UUID holdId, long showId, String status, long totalMinor, String currency,
            Instant createdAt, List<HoldItem> items) {
    }

    @GetMapping("/me/bookings")
    List<BookingView> mine(@AuthenticationPrincipal Jwt jwt) {
        return jdbc.sql("""
                        SELECT id, hold_id, show_id, status, total_minor, currency, created_at
                        FROM bookings WHERE user_id = :u ORDER BY created_at DESC LIMIT 100""")
                .param("u", CurrentUser.id(jwt))
                .query((rs, i) -> {
                    UUID holdId = rs.getObject("hold_id", UUID.class);
                    return new BookingView(rs.getObject("id", UUID.class), holdId, rs.getLong("show_id"),
                            rs.getString("status"), rs.getLong("total_minor"), rs.getString("currency"),
                            rs.getTimestamp("created_at").toInstant(), reservations.items(holdId));
                })
                .list();
    }
}
