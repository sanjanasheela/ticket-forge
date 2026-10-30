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
import com.ticketforge.security.TokenService;

/**
 * Base for tests against the whole service and a real Postgres. One Spring context (and one
 * container) is shared by all subclasses, so tests create their own data (unique emails).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@Import(TestcontainersConfig.class)
public abstract class IntegrationTest {

    @Autowired
    protected TokenService tokens;

    @Autowired
    protected MockMvcTester mvc;
    @Autowired
    protected JdbcClient jdbc;

    /** Catalog does not log users in (auth-service does); tests mint tokens with the shared key, as any service could verify. */
    protected String adminToken() {
        return tokens.issueAccess(UUID.randomUUID(), "ADMIN");
    }

    protected String userToken() {
        return tokens.issueAccess(UUID.randomUUID(), "USER");
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

    protected static String body(MvcTestResult result) {
        try {
            return result.getResponse().getContentAsString();
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }
}
