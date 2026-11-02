package com.ticketforge.payment;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ticketforge.shared.ApiException;
import com.ticketforge.shared.Sql;

/** Payment records. Every state change is a guarded UPDATE, so concurrent resolvers cannot both act. */
@Service
public class PaymentService {

    private final JdbcClient jdbc;
    private final PaymentGateway gateway;
    private final Clock clock;

    PaymentService(JdbcClient jdbc, PaymentGateway gateway, Clock clock) {
        this.jdbc = jdbc;
        this.gateway = gateway;
        this.clock = clock;
    }

    public PaymentGateway gateway() {
        return gateway;
    }

    @Transactional
    public UUID createPending(UUID holdId, UUID userId, long amountMinor, String currency) {
        UUID id = UUID.randomUUID();
        try {
            jdbc.sql("""
                            INSERT INTO payments (id, hold_id, user_id, amount_minor, currency, status, created_at)
                            VALUES (:id, :h, :u, :a, :c, 'PENDING', :now)""")
                    .param("id", id).param("h", holdId).param("u", userId).param("a", amountMinor).param("c", currency)
                    .param("now", Sql.ts(clock.instant()))
                    .update();
        } catch (DuplicateKeyException e) {
            throw ApiException.conflict("CHECKOUT_IN_PROGRESS", "This hold is already being paid for or has been paid");
        }
        return id;
    }

    /** PENDING -> SUCCEEDED. False if someone else already resolved it. */
    @Transactional
    public boolean markSucceeded(UUID paymentId, String gatewayRef) {
        return move(paymentId, "PENDING", "SUCCEEDED", gatewayRef, null);
    }

    @Transactional
    public boolean markFailed(UUID paymentId, String reason) {
        return move(paymentId, "PENDING", "FAILED", null, reason);
    }

    @Transactional
    public boolean markRefunded(UUID paymentId, String reason) {
        return move(paymentId, "SUCCEEDED", "REFUNDED", null, reason);
    }

    public record PendingPayment(UUID id, UUID holdId, UUID userId) {
    }

    public List<PendingPayment> pendingOlderThan(Instant cutoff, int limit) {
        return jdbc.sql("""
                        SELECT id, hold_id, user_id FROM payments
                        WHERE status = 'PENDING' AND created_at < :cutoff ORDER BY created_at LIMIT :n""")
                .param("cutoff", Sql.ts(cutoff)).param("n", limit)
                .query((rs, i) -> new PendingPayment(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class),
                        rs.getObject(3, UUID.class)))
                .list();
    }

    private boolean move(UUID id, String from, String to, String ref, String reason) {
        return jdbc.sql("""
                        UPDATE payments SET status = :to, gateway_ref = COALESCE(:ref, gateway_ref),
                               failure_reason = COALESCE(:reason, failure_reason), updated_at = :now
                        WHERE id = :id AND status = :from""")
                .param("to", to).param("ref", ref).param("reason", reason).param("now", Sql.ts(clock.instant()))
                .param("id", id).param("from", from)
                .update() == 1;
    }
}
