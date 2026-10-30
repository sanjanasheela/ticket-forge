package com.ticketforge.security;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Guards service-to-service endpoints under {@code /internal/**}. Callers must send the shared
 * {@code X-Internal-Token}. Kong never routes {@code /internal/**} to the outside, so this is the
 * second lock, not the only one. (Phase 3 can replace it with mTLS.)
 */
@Component
class InternalApiFilter extends OncePerRequestFilter {

    static final String HEADER = "X-Internal-Token";

    private final byte[] expected;

    InternalApiFilter(@Value("${ticketforge.internal.token:dev-internal-token-change-me}") String token) {
        this.expected = token.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/internal/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String given = request.getHeader(HEADER);
        // constant-time comparison: do not leak the token through response timing
        if (given == null || !MessageDigest.isEqual(expected, given.getBytes(StandardCharsets.UTF_8))) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "Internal endpoint");
            return;
        }
        chain.doFilter(request, response);
    }
}
