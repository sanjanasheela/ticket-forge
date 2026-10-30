package com.ticketforge.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.ticketforge.IntegrationTest;
import com.ticketforge.security.TokenService;

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
        String uri = "/admin/seed/loadtest-users?count=3";
        assertThat(postJson(uri, null, "")).hasStatus(401);
        assertThat(postJson(uri, newUser(), "")).hasStatus(403);
        assertThat(postJson(uri, adminToken(), "")).hasStatusOk();
    }

    @Test
    void queueTokenCannotBeUsedAsLogin() {
        String queueToken = tokens.issueQueueAccess(UUID.randomUUID(), 1, Duration.ofMinutes(5)).token();
        assertThat(postJson("/admin/seed/loadtest-users?count=1", queueToken, "")).hasStatus(401);
    }
}
