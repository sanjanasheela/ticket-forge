package com.ticketforge.catalog;

import java.util.Optional;

/**
 * Where catalog read responses are cached, as ready-to-send JSON. An interface on purpose: Phase 2
 * uses real Redis, Phase 3 swaps in the custom mini-Redis without touching the callers.
 *
 * <p>Contract: a cache problem must never fail a read. Implementations return empty / do nothing
 * on any error, so the caller falls through to PostgreSQL.
 */
public interface CatalogCache {

    Optional<String> get(String key);

    void put(String key, String json);

    void evict(String... keys);
}
