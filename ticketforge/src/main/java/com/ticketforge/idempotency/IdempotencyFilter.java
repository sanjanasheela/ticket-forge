package com.ticketforge.idempotency;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingResponseWrapper;

import com.ticketforge.idempotency.IdempotencyStore.Stored;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Idempotency-Key handling (spec §8) for the POSTs that create things. Runs after Spring Security
 * (servlet filter order), so keys are scoped per user: two users can never collide.
 * <ul>
 * <li>new key: run the request, remember the response (unless 5xx)</li>
 * <li>same key, same body, finished: replay the stored response</li>
 * <li>same key, still running: 409 REQUEST_IN_PROGRESS</li>
 * <li>same key, different body: 422 IDEMPOTENCY_KEY_REUSED</li>
 * </ul>
 */
@Component
class IdempotencyFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyFilter.class);

    static final String HEADER = "Idempotency-Key";
    private static final Pattern OPTIONAL = Pattern.compile("^/(auth/signup|shows/\\d+/holds)$");
    private static final Pattern REQUIRED = Pattern.compile("^/holds/[0-9a-fA-F-]{36}/checkout$");

    private final IdempotencyStore store;
    private final MeterRegistry meters;

    IdempotencyFilter(IdempotencyStore store, MeterRegistry meters) {
        this.store = store;
        this.meters = meters;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI().substring(request.getContextPath().length());
        return !"POST".equals(request.getMethod()) || !(OPTIONAL.matcher(path).matches() || REQUIRED.matcher(path).matches());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String key = request.getHeader(HEADER);
        String path = request.getRequestURI().substring(request.getContextPath().length());
        if (key == null || key.isBlank()) {
            if (REQUIRED.matcher(path).matches()) {
                writeError(response, HttpStatus.BAD_REQUEST, "IDEMPOTENCY_KEY_REQUIRED", "Send an Idempotency-Key header");
                return;
            }
            chain.doFilter(request, response);
            return;
        }
        if (key.length() > 200) {
            writeError(response, HttpStatus.BAD_REQUEST, "IDEMPOTENCY_KEY_INVALID", "Idempotency-Key is too long");
            return;
        }

        byte[] body = request.getInputStream().readAllBytes();
        String scope = scope();
        String hash = sha256(request.getMethod() + " " + path + "\n" + new String(body, StandardCharsets.UTF_8));

        boolean owner;
        Optional<Stored> stored = Optional.empty();
        try {
            owner = store.begin(scope, key, hash);
            if (!owner) {
                stored = store.find(scope, key);
            }
        } catch (DataAccessException e) {
            // Runs before Spring MVC, so the error handler never sees this: answer 503 ourselves.
            count("overloaded");
            response.setHeader("Retry-After", "1");
            writeError(response, HttpStatus.SERVICE_UNAVAILABLE, "OVERLOADED", "Try again shortly");
            return;
        }
        if (!owner) {
            if (stored.isEmpty()) { // abandoned between our insert and read: treat as in progress
                count("in_progress");
                writeError(response, HttpStatus.CONFLICT, "REQUEST_IN_PROGRESS", "A request with this key is in progress");
                return;
            }
            Stored s = stored.get();
            if (!s.requestHash().equals(hash)) {
                count("mismatch");
                writeError(response, HttpStatus.UNPROCESSABLE_CONTENT, "IDEMPOTENCY_KEY_REUSED",
                        "This Idempotency-Key was used for a different request");
            } else if (!"COMPLETED".equals(s.status())) {
                count("in_progress");
                writeError(response, HttpStatus.CONFLICT, "REQUEST_IN_PROGRESS", "A request with this key is in progress");
            } else {
                count("replay");
                response.setStatus(s.responseCode());
                response.setHeader("Idempotent-Replayed", "true");
                if (s.contentType() != null) {
                    response.setContentType(s.contentType());
                }
                if (s.responseBody() != null) {
                    response.getOutputStream().write(s.responseBody().getBytes(StandardCharsets.UTF_8));
                }
            }
            return;
        }

        ContentCachingResponseWrapper wrapped = new ContentCachingResponseWrapper(response);
        boolean succeeded = false;
        try {
            chain.doFilter(new CachedBodyRequest(request, body), wrapped);
            succeeded = wrapped.getStatus() < 500;
        } finally {
            try {
                if (succeeded) {
                    store.complete(scope, key, wrapped.getStatus(),
                            new String(wrapped.getContentAsByteArray(), StandardCharsets.UTF_8), wrapped.getContentType());
                } else {
                    store.abandon(scope, key);
                }
            } catch (DataAccessException e) {
                // The request itself is done; don't turn its answer into an error. The key stays
                // IN_PROGRESS (retries get 409) until cleanup removes it after 5 minutes.
                log.warn("Could not record idempotency outcome scope={} key={}: {}", scope, key, e.toString());
            }
            wrapped.copyBodyToResponse();
        }
    }

    private void count(String outcome) {
        meters.counter("ticketforge.idempotency", "outcome", outcome).increment();
    }

    private static String scope() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return auth != null && auth.isAuthenticated() && !"anonymousUser".equals(auth.getName()) ? auth.getName() : "anonymous";
    }

    private static void writeError(HttpServletResponse response, HttpStatus status, String code, String message)
            throws IOException {
        response.setStatus(status.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write("{\"code\":\"%s\",\"message\":\"%s\",\"details\":{}}".formatted(code, message));
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Lets the controller read a body this filter already consumed. */
    private static final class CachedBodyRequest extends HttpServletRequestWrapper {

        private final byte[] body;

        CachedBodyRequest(HttpServletRequest request, byte[] body) {
            super(request);
            this.body = body;
        }

        @Override
        public ServletInputStream getInputStream() {
            ByteArrayInputStream in = new ByteArrayInputStream(body);
            return new ServletInputStream() {
                @Override
                public boolean isFinished() {
                    return in.available() == 0;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(ReadListener listener) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public int read() {
                    return in.read();
                }

                @Override
                public int read(byte[] b, int off, int len) {
                    return in.read(b, off, len);
                }
            };
        }

        @Override
        public java.io.BufferedReader getReader() {
            return new java.io.BufferedReader(new java.io.InputStreamReader(getInputStream(), StandardCharsets.UTF_8));
        }
    }
}
