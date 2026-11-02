package com.ticketforge.idempotency;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.ticketforge.shared.Sql;

/** Autocommit statements only: the key must be visible to a concurrent retry immediately. */
@Component
class IdempotencyStore {

    private final JdbcClient jdbc;
    private final Clock clock;

    IdempotencyStore(JdbcClient jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    record Stored(String requestHash, String status, Integer responseCode, String responseBody, String contentType) {
    }

    /** True if this call now owns the key; otherwise returns what is stored. */
    boolean begin(String scope, String key, String requestHash) {
        return jdbc.sql("""
                        INSERT INTO idempotency_keys (scope, idem_key, request_hash, status, created_at)
                        VALUES (:s, :k, :h, 'IN_PROGRESS', :now) ON CONFLICT DO NOTHING""")
                .param("s", scope).param("k", key).param("h", requestHash).param("now", Sql.ts(clock.instant()))
                .update() == 1;
    }

    Optional<Stored> find(String scope, String key) {
        return jdbc.sql("""
                        SELECT request_hash, status, response_code, response_body, content_type
                        FROM idempotency_keys WHERE scope = :s AND idem_key = :k""")
                .param("s", scope).param("k", key)
                .query((rs, i) -> new Stored(rs.getString(1), rs.getString(2), rs.getObject(3, Integer.class),
                        rs.getString(4), rs.getString(5)))
                .optional();
    }

    void complete(String scope, String key, int code, String body, String contentType) {
        jdbc.sql("""
                        UPDATE idempotency_keys SET status = 'COMPLETED', response_code = :c, response_body = :b, content_type = :ct
                        WHERE scope = :s AND idem_key = :k""")
                .param("c", code).param("b", body).param("ct", contentType).param("s", scope).param("k", key)
                .update();
    }

    /** Server errors are not remembered: the client should be able to retry them. */
    void abandon(String scope, String key) {
        jdbc.sql("DELETE FROM idempotency_keys WHERE scope = :s AND idem_key = :k").param("s", scope).param("k", key).update();
    }

    @Scheduled(fixedDelay = 600_000)
    void cleanup() {
        var now = clock.instant();
        jdbc.sql("""
                        DELETE FROM idempotency_keys
                        WHERE created_at < :day OR (status = 'IN_PROGRESS' AND created_at < :stuck)""")
                .param("day", Sql.ts(now.minus(Duration.ofHours(24))))
                .param("stuck", Sql.ts(now.minus(Duration.ofMinutes(5))))
                .update();
    }
}
