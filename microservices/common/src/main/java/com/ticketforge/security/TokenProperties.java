package com.ticketforge.security;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * JWT settings every service shares. Other keys under the same prefix (refresh-token TTL,
 * bootstrap admin) belong to auth-service alone and are ignored here.
 */
@ConfigurationProperties("ticketforge.auth")
public record TokenProperties(String jwtSecret, Duration accessTokenTtl) {
}
