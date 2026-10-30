package com.ticketforge;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.jayway.jsonpath.JsonPath;

/**
 * Base for tests against the whole service and a real Postgres. One Spring context (and one
 * container) is shared by all subclasses, so tests create their own data (unique emails).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@Import(TestcontainersConfig.class)
public abstract class IntegrationTest {

    @Autowired
    protected MockMvcTester mvc;
    @Autowired
    protected JdbcClient jdbc;

    private String adminToken;

    protected String adminToken() {
        if (adminToken == null) {
            adminToken = login("admin@ticketforge.local", "admin12345");
        }
        return adminToken;
    }

    protected String login(String email, String password) {
        MvcTestResult r = postJson("/auth/login", null, """
                {"email":"%s","password":"%s"}""".formatted(email, password));
        assertThat(r).hasStatusOk();
        return read(r, "$.accessToken");
    }

    /** Signs up a fresh user and returns its access token. */
    protected String newUser() {
        MvcTestResult r = postJson("/auth/signup", null, """
                {"email":"user-%s@test.local","password":"password123"}""".formatted(UUID.randomUUID()));
        assertThat(r).hasStatus(201);
        return read(r, "$.accessToken");
    }

    protected MvcTestResult postJson(String uri, String token, String body) {
        var req = mvc.post().uri(uri).contentType(MediaType.APPLICATION_JSON).content(body);
        if (token != null) {
            req = req.header("Authorization", "Bearer " + token);
        }
        return req.exchange();
    }

    protected MvcTestResult get(String uri, String token) {
        var req = mvc.get().uri(uri);
        if (token != null) {
            req = req.header("Authorization", "Bearer " + token);
        }
        return req.exchange();
    }

    protected static <T> T read(MvcTestResult result, String path) {
        try {
            return JsonPath.read(result.getResponse().getContentAsString(), path);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }
}
