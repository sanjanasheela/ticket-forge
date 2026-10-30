package com.ticketforge.catalog;

import java.time.Duration;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Cache-aside on Redis with a short TTL. Every call is wrapped: Redis being down or slow costs a
 * Postgres read, never a failed request. The client timeout (see application.yml) keeps a dead
 * Redis from stalling requests.
 */
@Component
@ConditionalOnProperty(name = "ticketforge.cache.type", havingValue = "redis", matchIfMissing = true)
class RedisCatalogCache implements CatalogCache {

    private static final Logger log = LoggerFactory.getLogger(RedisCatalogCache.class);

    private final StringRedisTemplate redis;
    private final Duration ttl;
    private final MeterRegistry meters;

    RedisCatalogCache(StringRedisTemplate redis, MeterRegistry meters,
            @Value("${ticketforge.cache.ttl:30s}") Duration ttl) {
        this.redis = redis;
        this.meters = meters;
        this.ttl = ttl;
    }

    @Override
    public Optional<String> get(String key) {
        try {
            String value = redis.opsForValue().get(key);
            meters.counter("ticketforge.catalog.cache", "result", value == null ? "miss" : "hit").increment();
            return Optional.ofNullable(value);
        } catch (RuntimeException e) {
            degraded("get", e);
            return Optional.empty();
        }
    }

    @Override
    public void put(String key, String json) {
        try {
            redis.opsForValue().set(key, json, ttl);
        } catch (RuntimeException e) {
            degraded("put", e);
        }
    }

    @Override
    public void evict(String... keys) {
        try {
            redis.delete(java.util.List.of(keys));
        } catch (RuntimeException e) {
            // Stale entries then live at most one TTL; still bounded, never wrong forever.
            degraded("evict", e);
        }
    }

    private void degraded(String op, RuntimeException e) {
        meters.counter("ticketforge.catalog.cache", "result", "error").increment();
        log.warn("Catalog cache {} failed, falling back to PostgreSQL: {}", op, e.toString());
    }
}
