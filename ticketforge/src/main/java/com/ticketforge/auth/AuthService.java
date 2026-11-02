package com.ticketforge.auth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ticketforge.shared.ApiException;

@Service
class AuthService {

    private static final Logger log = LoggerFactory.getLogger(AuthService.class);
    private static final SecureRandom RANDOM = new SecureRandom();

    private final JdbcClient jdbc;
    private final PasswordEncoder passwords;
    private final TokenService tokens;
    private final AuthProperties props;
    private final Clock clock;
    /** Compared against when the email is unknown, so login timing doesn't reveal which emails exist. */
    private final String dummyHash;

    AuthService(JdbcClient jdbc, PasswordEncoder passwords, TokenService tokens, AuthProperties props, Clock clock) {
        this.jdbc = jdbc;
        this.passwords = passwords;
        this.tokens = tokens;
        this.props = props;
        this.clock = clock;
        this.dummyHash = passwords.encode("not-a-real-password");
    }

    @Transactional
    Tokens signup(String email, String password) {
        UUID id;
        try {
            id = jdbc.sql("INSERT INTO users (email, password_hash, role) VALUES (:email, :hash, 'USER') RETURNING id")
                    .param("email", email.trim())
                    .param("hash", passwords.encode(password))
                    .query(UUID.class)
                    .single();
        } catch (DuplicateKeyException e) {
            throw ApiException.conflict("EMAIL_TAKEN", "An account with this email already exists");
        }
        log.info("User signed up user_id={}", id);
        return issue(id, "USER");
    }

    @Transactional
    Tokens login(String email, String password) {
        Optional<UserRow> user = jdbc.sql("SELECT id, password_hash, role FROM users WHERE lower(email) = lower(:email)")
                .param("email", email.trim())
                .query(UserRow.class)
                .optional();
        String hash = user.map(UserRow::passwordHash).orElse(dummyHash);
        if (!passwords.matches(password, hash) || user.isEmpty()) {
            throw invalidCredentials();
        }
        return issue(user.get().id(), user.get().role());
    }

    /**
     * Rotates the refresh token: the presented one is revoked and a new pair issued. Presenting an
     * already-revoked token means it was stolen or replayed, so every session of that user is revoked.
     */
    @Transactional(noRollbackFor = ApiException.class)
    Tokens refresh(String refreshToken) {
        Instant now = clock.instant();
        String hash = sha256(refreshToken);
        Optional<UUID> userId = jdbc.sql("""
                        UPDATE refresh_tokens SET revoked_at = :now
                        WHERE token_hash = :hash AND revoked_at IS NULL AND expires_at > :now
                        RETURNING user_id""")
                .param("now", ts(now))
                .param("hash", hash)
                .query(UUID.class)
                .optional();
        if (userId.isEmpty()) {
            jdbc.sql("SELECT user_id FROM refresh_tokens WHERE token_hash = :hash AND revoked_at IS NOT NULL")
                    .param("hash", hash)
                    .query(UUID.class)
                    .optional()
                    .ifPresent(reused -> {
                        log.warn("Refresh token reuse detected, revoking all sessions user_id={}", reused);
                        revokeAll(reused, now);
                    });
            throw new ApiException(HttpStatus.UNAUTHORIZED, "INVALID_REFRESH_TOKEN", "Refresh token is invalid or expired");
        }
        String role = jdbc.sql("SELECT role FROM users WHERE id = :id").param("id", userId.get()).query(String.class).single();
        return issue(userId.get(), role);
    }

    @Transactional
    void logout(String refreshToken) {
        jdbc.sql("UPDATE refresh_tokens SET revoked_at = :now WHERE token_hash = :hash AND revoked_at IS NULL")
                .param("now", ts(clock.instant()))
                .param("hash", sha256(refreshToken))
                .update();
    }

    private void revokeAll(UUID userId, Instant now) {
        jdbc.sql("UPDATE refresh_tokens SET revoked_at = :now WHERE user_id = :uid AND revoked_at IS NULL")
                .param("now", ts(now))
                .param("uid", userId)
                .update();
    }

    private Tokens issue(UUID userId, String role) {
        byte[] raw = new byte[32];
        RANDOM.nextBytes(raw);
        String refresh = Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
        jdbc.sql("INSERT INTO refresh_tokens (user_id, token_hash, expires_at) VALUES (:uid, :hash, :exp)")
                .param("uid", userId)
                .param("hash", sha256(refresh))
                .param("exp", ts(clock.instant().plus(props.refreshTokenTtl())))
                .update();
        return new Tokens(tokens.issueAccess(userId, role), refresh, tokens.accessTtlSeconds(), "Bearer");
    }

    private static ApiException invalidCredentials() {
        return new ApiException(HttpStatus.UNAUTHORIZED, "INVALID_CREDENTIALS", "Email or password is incorrect");
    }

    private static java.sql.Timestamp ts(Instant i) { // pgjdbc has no Instant binding
        return java.sql.Timestamp.from(i);
    }

    static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    record UserRow(UUID id, String passwordHash, String role) {
    }

    record Tokens(String accessToken, String refreshToken, long expiresIn, String tokenType) {
    }
}
