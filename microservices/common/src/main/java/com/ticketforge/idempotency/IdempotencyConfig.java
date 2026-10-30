package com.ticketforge.idempotency;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Idempotency is opt-in per service ({@code ticketforge.idempotency.enabled=true}) because it needs
 * its own {@code idempotency_keys} table. Only the services that accept retried writes turn it on.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "ticketforge.idempotency.enabled", havingValue = "true")
@EnableScheduling
class IdempotencyConfig {
}
