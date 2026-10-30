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

import com.ticketforge.contracts.SagaMessages.HoldInfo;
import com.ticketforge.shared.CurrentUser;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

@RestController
class BookingController {

    private final CheckoutSaga saga;
    private final JdbcClient jdbc;
    private final JsonMapper json;

    BookingController(CheckoutSaga saga, JdbcClient jdbc, JsonMapper json) {
        this.saga = saga;
        this.jdbc = jdbc;
        this.json = json;
    }

    /** Requires an Idempotency-Key header (enforced by the idempotency filter). Same statuses as Phase 1. */
    @PostMapping("/holds/{id}/checkout")
    ResponseEntity<CheckoutResult> checkout(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        CheckoutResult r = saga.checkout(CurrentUser.id(jwt), id);
        HttpStatus status = switch (r.outcome()) {
            case CONFIRMED -> HttpStatus.CREATED;
            case PAYMENT_PENDING -> HttpStatus.ACCEPTED;
            case PAYMENT_FAILED -> HttpStatus.PAYMENT_REQUIRED;
            case HOLD_EXPIRED_REFUNDED -> HttpStatus.CONFLICT;
        };
        return ResponseEntity.status(status).body(r);
    }

    record BookingView(UUID id, UUID holdId, long showId, String status, long totalMinor, String currency,
            Instant createdAt, List<HoldInfo.Item> items) {
    }

    /** Items come from the booking's own copy of what was bought; no call to reservation-service. */
    @GetMapping("/me/bookings")
    List<BookingView> mine(@AuthenticationPrincipal Jwt jwt) {
        return jdbc.sql("""
                        SELECT id, hold_id, show_id, status, total_minor, currency, items, created_at
                        FROM bookings WHERE user_id = :u ORDER BY created_at DESC LIMIT 100""")
                .param("u", CurrentUser.id(jwt))
                .query((rs, i) -> new BookingView(rs.getObject("id", UUID.class), rs.getObject("hold_id", UUID.class),
                        rs.getLong("show_id"), rs.getString("status"), rs.getLong("total_minor"), rs.getString("currency"),
                        rs.getTimestamp("created_at").toInstant(),
                        json.readValue(rs.getString("items"), new TypeReference<List<HoldInfo.Item>>() { })))
                .list();
    }
}
