package com.ticketforge.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.ticketforge.IntegrationTest;

class AuthIntegrationTests extends IntegrationTest {

    @Autowired
    TokenService tokens;

    @Test
    void signupThenLoginThenDuplicateEmailRejected() {
        String email = "alice-" + UUID.randomUUID() + "@test.local";
        String creds = """
                {"email":"%s","password":"password123"}""".formatted(email);

        assertThat(postJson("/auth/signup", null, creds)).hasStatus(201);
        assertThat(postJson("/auth/login", null, creds)).hasStatusOk().bodyJson().extractingPath("$.tokenType").isEqualTo("Bearer");
        assertThat(postJson("/auth/signup", null, creds.replace(email, email.toUpperCase())))
                .hasStatus(409).bodyJson().extractingPath("$.code").isEqualTo("EMAIL_TAKEN");
    }

    @Test
    void wrongPasswordIsUnauthorized() {
        assertThat(postJson("/auth/login", null, """
                {"email":"admin@ticketforge.local","password":"wrong-password"}"""))
                .hasStatus(401).bodyJson().extractingPath("$.code").isEqualTo("INVALID_CREDENTIALS");
    }

    @Test
    void refreshRotatesAndReuseRevokesEverySession() {
        MvcTestResult signup = postJson("/auth/signup", null, """
                {"email":"bob-%s@test.local","password":"password123"}""".formatted(UUID.randomUUID()));
        String first = read(signup, "$.refreshToken");

        MvcTestResult rotated = postJson("/auth/refresh", null, "{\"refreshToken\":\"%s\"}".formatted(first));
        assertThat(rotated).hasStatusOk();
        String second = read(rotated, "$.refreshToken");
        assertThat(second).isNotEqualTo(first);

        // Replaying the old token is treated as theft: rejected, and the new token dies too.
        assertThat(postJson("/auth/refresh", null, "{\"refreshToken\":\"%s\"}".formatted(first))).hasStatus(401);
        assertThat(postJson("/auth/refresh", null, "{\"refreshToken\":\"%s\"}".formatted(second))).hasStatus(401);
    }

    @Test
    void logoutRevokesRefreshToken() {
        MvcTestResult signup = postJson("/auth/signup", null, """
                {"email":"carol-%s@test.local","password":"password123"}""".formatted(UUID.randomUUID()));
        String refresh = read(signup, "$.refreshToken");
        assertThat(postJson("/auth/logout", null, "{\"refreshToken\":\"%s\"}".formatted(refresh))).hasStatus(204);
        assertThat(postJson("/auth/refresh", null, "{\"refreshToken\":\"%s\"}".formatted(refresh))).hasStatus(401);
    }

    @Test
    void adminEndpointsRequireAdminRole() {
        String body = """
                {"title":"X","genre":"Y","durationMin":90,"category":"MOVIE"}""";
        assertThat(postJson("/admin/events", null, body)).hasStatus(401);
        assertThat(postJson("/admin/events", newUser(), body)).hasStatus(403);
        assertThat(postJson("/admin/events", adminToken(), body)).hasStatus(201);
    }

    @Test
    void queueTokenCannotBeUsedAsLogin() {
        String queueToken = tokens.issueQueueAccess(UUID.randomUUID(), 1, Duration.ofMinutes(5)).token();
        assertThat(get("/me/bookings", queueToken)).hasStatus(401);
    }
}
