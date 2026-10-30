package com.ticketforge.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.core.StringRedisTemplate;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class CatalogCacheTests extends CatalogTestSupport {

    @Autowired
    StringRedisTemplate redis;

    @Test
    void firstReadFillsTheCacheAndTheSecondIsServedFromIt() {
        Created c = createShowFixture("MOVIE");
        String key = "catalog:show:" + c.showId();
        assertThat(redis.hasKey(key)).as("nothing cached before the first read").isFalse();

        String first = body(get("/shows/" + c.showId(), null));
        assertThat(redis.hasKey(key)).as("filled by the first read").isTrue();
        assertThat(redis.opsForValue().get(key)).isEqualTo(first);
        assertThat(body(get("/shows/" + c.showId(), null))).isEqualTo(first);
    }

    @Test
    void creatingAnEventEvictsTheCachedEventLists() {
        createEvent("CONCERT");
        String before = body(get("/events?category=CONCERT", null));
        assertThat(redis.hasKey("catalog:events:CONCERT")).isTrue();

        long newId = createEvent("CONCERT");

        assertThat(redis.hasKey("catalog:events:CONCERT")).as("evicted by the write").isFalse();
        String after = body(get("/events?category=CONCERT", null));
        assertThat(after).isNotEqualTo(before).contains("\"id\":" + newId);
    }

    @Test
    void changingPricesEvictsTheCachedShow() {
        Created c = createShowFixture("MOVIE");
        get("/shows/" + c.showId(), null);
        assertThat(redis.hasKey("catalog:show:" + c.showId())).isTrue();

        assertThat(mvc.put().uri("/admin/shows/" + c.showId() + "/prices")
                .contentType(org.springframework.http.MediaType.APPLICATION_JSON)
                .header("Authorization", "Bearer " + adminToken())
                .content("[{\"tierId\":" + c.tierId() + ",\"priceMinor\":99900}]").exchange()).hasStatus(204);

        assertThat(redis.hasKey("catalog:show:" + c.showId())).isFalse();
        assertThat(body(get("/shows/" + c.showId(), null))).contains("99900");
    }

    /** The promise that makes the cache safe to deploy: Redis trouble costs speed, never availability. */
    @Test
    void anUnreachableRedisFallsThroughInsteadOfFailing() {
        RedisStandaloneConfiguration cfg = new RedisStandaloneConfiguration("localhost", 1);   // nothing listens here
        LettuceConnectionFactory dead = new LettuceConnectionFactory(cfg, LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(200)).build());
        dead.afterPropertiesSet();
        try {
            StringRedisTemplate template = new StringRedisTemplate(dead);
            RedisCatalogCache cache = new RedisCatalogCache(template, new SimpleMeterRegistry(), Duration.ofSeconds(30), Duration.ofSeconds(5));

            assertThat(cache.get("k")).isEmpty();
            cache.put("k", "v");        // must not throw
            cache.evict("k");           // must not throw

            // The circuit breaker is now open: further calls must not pay the timeout again.
            long start = System.nanoTime();
            for (int i = 0; i < 100; i++) {
                assertThat(cache.get("k")).isEmpty();
                cache.put("k", "v");
            }
            assertThat(Duration.ofNanos(System.nanoTime() - start)).as("200 calls while the breaker is open").isLessThan(Duration.ofMillis(100));
        } finally {
            dead.destroy();
        }
    }
}
