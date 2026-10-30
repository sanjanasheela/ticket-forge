package com.ticketforge.catalog;

import java.util.function.Supplier;

import org.springframework.stereotype.Service;


import tools.jackson.databind.json.JsonMapper;

/**
 * The public read path: cache first, PostgreSQL on a miss. Responses are cached as JSON text, so a
 * hit is returned without deserialising. Cache keys are listed here and nowhere else.
 */
@Service
class CachedCatalog {

    private final CatalogCache cache;
    private final CatalogQueries queries;
    private final JsonMapper json;

    CachedCatalog(CatalogCache cache, CatalogQueries queries, JsonMapper json) {
        this.cache = cache;
        this.queries = queries;
        this.json = json;
    }

    String events(ShowCategory category) {
        return cached(eventsKey(category), () -> queries.events(category));
    }

    String showsOfEvent(long eventId) {
        return cached(eventShowsKey(eventId), () -> queries.showsOfEvent(eventId));
    }

    String show(long showId) {
        return cached(showKey(showId), () -> queries.show(showId));
    }

    private String cached(String key, Supplier<Object> loader) {
        return cache.get(key).orElseGet(() -> {
            String body = json.writeValueAsString(loader.get());
            cache.put(key, body);
            return body;
        });
    }

    // ---- invalidation: called by the controller after a write has committed ----------------------

    void eventCreated() {
        cache.evict(eventsKey(null), eventsKey(ShowCategory.MOVIE), eventsKey(ShowCategory.CONCERT));
    }

    void showChanged(long showId, Long eventId) {
        if (eventId == null) {
            cache.evict(showKey(showId));
        } else {
            cache.evict(showKey(showId), eventShowsKey(eventId));
        }
    }

    private static String eventsKey(ShowCategory category) {
        return "catalog:events:" + (category == null ? "all" : category.name());
    }

    private static String eventShowsKey(long eventId) {
        return "catalog:event-shows:" + eventId;
    }

    private static String showKey(long showId) {
        return "catalog:show:" + showId;
    }
}
