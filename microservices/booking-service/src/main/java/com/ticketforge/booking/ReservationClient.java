package com.ticketforge.booking;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

import com.ticketforge.contracts.SagaMessages.HoldInfo;
import com.ticketforge.shared.ApiException;

/**
 * Step 1 of the saga: is this hold still live, and what does it cost? Fails <em>closed</em>: if
 * reservation-service cannot be reached the buyer gets a 503 before anyone is charged.
 */
@Component
class ReservationClient {

    private final RestClient http;

    ReservationClient(@Value("${ticketforge.reservation.url:http://localhost:8083}") String baseUrl,
            @Value("${ticketforge.internal.token:dev-internal-token-change-me}") String token) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(1_000);
        factory.setReadTimeout(3_000);
        this.http = RestClient.builder().baseUrl(baseUrl).requestFactory(factory).defaultHeader("X-Internal-Token", token).build();
    }

    HoldInfo verify(UUID holdId, UUID userId) {
        try {
            return http.get().uri("/internal/holds/{id}?userId={u}", holdId, userId).retrieve().body(HoldInfo.class);
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().isSameCodeAs(HttpStatus.CONFLICT)) {
                throw new ApiException(HttpStatus.CONFLICT, "HOLD_EXPIRED", "This hold has expired or was released");
            }
            if (e.getStatusCode().isSameCodeAs(HttpStatus.NOT_FOUND)) {
                throw ApiException.notFound("HOLD_NOT_FOUND", "Hold not found");
            }
            throw unavailable(e);
        } catch (RestClientException e) {
            throw unavailable(e);
        }
    }

    private static ApiException unavailable(Exception e) {
        return new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "RESERVATION_UNAVAILABLE",
                "Could not check your hold right now; nothing was charged. Please try again.");
    }
}
