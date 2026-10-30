package com.ticketforge.booking;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.ticketforge.booking.Saga.State;
import com.ticketforge.shared.Sql;

/**
 * Persistence of saga state. Every transition is a guarded UPDATE ({@code WHERE state = <expected>}),
 * so two actors racing on one saga (the HTTP thread, the Kafka reply, the reconciler, the recovery job)
 * cannot both win: exactly one sees "1 row updated".
 */
@Repository
class SagaRepository {

    private static final String COLUMNS = """
            id, hold_id, user_id, show_id, total_minor, currency, items, payment_id, gateway_ref, state, booking_id, reason""";

    private final JdbcClient jdbc;
    private final Clock clock;

    SagaRepository(JdbcClient jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    Optional<Saga> findByHold(UUID holdId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM sagas WHERE hold_id = :h").param("h", holdId)
                .query(SagaRepository::map).optional();
    }

    Optional<Saga> find(UUID sagaId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM sagas WHERE id = :id").param("id", sagaId)
                .query(SagaRepository::map).optional();
    }

    /** Row-locks the saga for the rest of the caller's transaction. */
    Optional<Saga> lock(UUID sagaId) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM sagas WHERE id = :id FOR UPDATE").param("id", sagaId)
                .query(SagaRepository::map).optional();
    }

    /**
     * Starts a saga for a hold, or restarts one whose last attempt was declined (state FAILED).
     * @return the saga id, or empty if a saga for this hold is already in another state
     */
    Optional<UUID> start(UUID holdId, UUID userId, long showId, long totalMinor, String currency, String itemsJson) {
        Instant now = clock.instant();
        return jdbc.sql("""
                        INSERT INTO sagas (id, hold_id, user_id, show_id, total_minor, currency, items, state, created_at, updated_at)
                        VALUES (:id, :h, :u, :s, :t, :c, :items, 'CHARGING', :now, :now)
                        ON CONFLICT (hold_id) DO UPDATE
                           SET state = 'CHARGING', payment_id = NULL, gateway_ref = NULL, reason = NULL, updated_at = :now
                           WHERE sagas.state = 'FAILED' AND sagas.user_id = EXCLUDED.user_id
                        RETURNING id""")
                .param("id", UUID.randomUUID()).param("h", holdId).param("u", userId).param("s", showId)
                .param("t", totalMinor).param("c", currency).param("items", itemsJson).param("now", Sql.ts(now))
                .query(UUID.class).optional();
    }

    void attachPayment(UUID sagaId, UUID paymentId) {
        jdbc.sql("UPDATE sagas SET payment_id = :p, updated_at = :now WHERE id = :id")
                .param("p", paymentId).param("id", sagaId).param("now", Sql.ts(clock.instant())).update();
    }

    /** @return true if the saga was in one of {@code from} and is now in {@code to} */
    boolean move(UUID sagaId, List<State> from, State to, String gatewayRef, String reason) {
        return jdbc.sql("""
                        UPDATE sagas SET state = :to, gateway_ref = COALESCE(:ref, gateway_ref),
                               reason = COALESCE(:reason, reason), updated_at = :now
                        WHERE id = :id AND state = ANY (string_to_array(:from, ','))""")
                .param("to", to.name()).param("ref", gatewayRef).param("reason", reason)
                .param("now", Sql.ts(clock.instant())).param("id", sagaId)
                .param("from", String.join(",", from.stream().map(Enum::name).toList()))
                .update() == 1;
    }

    void markBooked(UUID sagaId, UUID bookingId) {
        jdbc.sql("UPDATE sagas SET state = 'BOOKED', booking_id = :b, updated_at = :now WHERE id = :id")
                .param("b", bookingId).param("id", sagaId).param("now", Sql.ts(clock.instant())).update();
    }

    /** Touches {@code updated_at} so a resend or retry is not repeated on the very next tick. */
    void touch(UUID sagaId) {
        jdbc.sql("UPDATE sagas SET updated_at = :now WHERE id = :id").param("id", sagaId).param("now", Sql.ts(clock.instant())).update();
    }

    List<Saga> stuck(State state, Instant olderThan, int limit) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM sagas WHERE state = :s AND updated_at < :cutoff ORDER BY updated_at LIMIT :n")
                .param("s", state.name()).param("cutoff", Sql.ts(olderThan)).param("n", limit)
                .query(SagaRepository::map).list();
    }

    private static Saga map(ResultSet rs, int row) throws SQLException {
        return new Saga(rs.getObject("id", UUID.class), rs.getObject("hold_id", UUID.class), rs.getObject("user_id", UUID.class),
                rs.getLong("show_id"), rs.getLong("total_minor"), rs.getString("currency"), rs.getString("items"),
                rs.getObject("payment_id", UUID.class), rs.getString("gateway_ref"), State.valueOf(rs.getString("state")),
                rs.getObject("booking_id", UUID.class), rs.getString("reason"));
    }
}
