package com.ticketforge.booking;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

/**
 * Lets the HTTP thread that started a checkout wait for the saga's outcome instead of polling.
 * Purely a latency optimisation: the database state is the truth, and a caller that times out
 * simply gets PAYMENT_PENDING and can look again.
 */
@Component
class ReplyWaiters {

    private final Map<UUID, CompletableFuture<Void>> waiting = new ConcurrentHashMap<>();

    CompletableFuture<Void> register(UUID sagaId) {
        return waiting.computeIfAbsent(sagaId, id -> new CompletableFuture<>());
    }

    void complete(UUID sagaId) {
        CompletableFuture<Void> f = waiting.remove(sagaId);
        if (f != null) {
            f.complete(null);
        }
    }

    void await(CompletableFuture<Void> future, UUID sagaId, Duration max) {
        try {
            future.get(max.toMillis(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            waiting.remove(sagaId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            waiting.remove(sagaId);
        } catch (java.util.concurrent.ExecutionException e) {
            waiting.remove(sagaId);
        }
    }
}
