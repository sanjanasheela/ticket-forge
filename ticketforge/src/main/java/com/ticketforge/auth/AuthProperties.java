package com.ticketforge.auth;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("ticketforge.auth")
record AuthProperties(String jwtSecret, Duration accessTokenTtl, Duration refreshTokenTtl, BootstrapAdmin bootstrapAdmin) {

    record BootstrapAdmin(String email, String password) {
    }
}
