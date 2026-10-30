package com.ticketforge.projection;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import com.ticketforge.contracts.VenueLayout;

/** Fetches a venue's seats from catalog-service's internal API. */
@Component
public class VenueLayoutClient {

    private final RestClient http;

    VenueLayoutClient(@Value("${ticketforge.catalog.url:http://localhost:8082}") String baseUrl,
            @Value("${ticketforge.internal.token:dev-internal-token-change-me}") String token) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(2_000);
        factory.setReadTimeout(60_000);   // a 50k-seat stadium is a few MB of JSON
        this.http = RestClient.builder().baseUrl(baseUrl).requestFactory(factory)
                .defaultHeader("X-Internal-Token", token).build();
    }

    public VenueLayout fetch(long venueId) {
        return http.get().uri("/internal/venues/{id}/layout", venueId).retrieve().body(VenueLayout.class);
    }
}
