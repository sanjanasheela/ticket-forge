package com.ticketforge.auth;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Settings only auth-service needs: refresh-token lifetime and the dev bootstrap admin. */
@ConfigurationProperties("ticketforge.auth")
record AuthServiceProperties(Duration refreshTokenTtl, BootstrapAdmin bootstrapAdmin) {

    record BootstrapAdmin(String email, String password) {
    }
}
