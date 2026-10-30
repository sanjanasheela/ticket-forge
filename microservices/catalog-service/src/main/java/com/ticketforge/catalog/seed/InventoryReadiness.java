package com.ticketforge.catalog.seed;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/** Waits until reservation-service has built the inventory for newly created shows. Used by the seeders only. */
@Component
class InventoryReadiness {

    private static final Logger log = LoggerFactory.getLogger(InventoryReadiness.class);

    private final RestClient http;
    private final Duration timeout;

    InventoryReadiness(@Value("${ticketforge.reservation.url:http://localhost:8083}") String baseUrl,
            @Value("${ticketforge.internal.token:dev-internal-token-change-me}") String token,
            @Value("${ticketforge.seed.inventory-timeout:180s}") Duration timeout) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(1_000);
        factory.setReadTimeout(5_000);
        this.http = RestClient.builder().baseUrl(baseUrl).requestFactory(factory).defaultHeader("X-Internal-Token", token).build();
        this.timeout = timeout;
    }

    void awaitReady(List<Long> showIds) {
        long deadline = System.nanoTime() + timeout.toNanos();
        for (long id : showIds) {
            while (!ready(id)) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("Inventory for show " + id + " not ready after " + timeout);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
        }
        log.info("Inventory ready for {} show(s)", showIds.size());
    }

    private boolean ready(long showId) {
        try {
            http.get().uri("/internal/shows/{id}/ready", showId).retrieve().toBodilessEntity();
            return true;
        } catch (RestClientException e) {
            return false;   // 404 = not built yet; connection errors are retried until the deadline
        }
    }
}
