package com.ticketforge.catalog;

import java.util.Optional;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Cache switched off ({@code ticketforge.cache.type=none}): every read goes to PostgreSQL. Used by tests and as the "no cache" benchmark. */
@Component
@ConditionalOnProperty(name = "ticketforge.cache.type", havingValue = "none")
class NoopCatalogCache implements CatalogCache {

    @Override
    public Optional<String> get(String key) {
        return Optional.empty();
    }

    @Override
    public void put(String key, String json) {
    }

    @Override
    public void evict(String... keys) {
    }
}
