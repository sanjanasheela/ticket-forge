package com.ticketforge.security;

import org.springframework.security.config.annotation.web.configurers.AuthorizeHttpRequestsConfigurer;

import org.springframework.security.config.annotation.web.builders.HttpSecurity;

/**
 * Each service states which of its routes are public (e.g. catalog reads) and which need a role.
 * Everything not matched here requires a valid access token, which the shared
 * {@link ResourceServerConfig} verifies locally with the common HS256 key: no call to auth-service.
 */
@FunctionalInterface
public interface ServiceRoutes {

    void configure(AuthorizeHttpRequestsConfigurer<HttpSecurity>.AuthorizationManagerRequestMatcherRegistry routes);
}
