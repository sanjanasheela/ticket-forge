package com.ticketforge.payment;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * In-process stand-in for a card processor. Latency is real (the calling thread sleeps), failures
 * are random, and a "timeout" is the nasty kind: the charge goes through but the response is lost,
 * so only {@link #lookup} can tell what happened.
 */
@Component
class MockPaymentGateway implements PaymentGateway {

    private final AtomicReference<GatewayConfig> config;
    private final Map<String, ChargeResult> processed = new ConcurrentHashMap<>();

    MockPaymentGateway(@Value("${ticketforge.payment.latency-ms}") long latencyMs,
            @Value("${ticketforge.payment.jitter-ms}") long jitterMs,
            @Value("${ticketforge.payment.failure-rate}") double failureRate,
            @Value("${ticketforge.payment.timeout-rate}") double timeoutRate) {
        this.config = new AtomicReference<>(new GatewayConfig(latencyMs, jitterMs, failureRate, timeoutRate));
    }

    GatewayConfig config() {
        return config.get();
    }

    void config(GatewayConfig c) {
        config.set(c);
    }

    @Override
    public ChargeResult charge(String idempotencyKey, long amountMinor, String currency) {
        GatewayConfig c = config.get();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        sleep(c.latencyMs() + (c.jitterMs() > 0 ? random.nextLong(c.jitterMs() + 1) : 0));

        ChargeResult result = processed.computeIfAbsent(idempotencyKey, k -> random.nextDouble() < c.failureRate()
                ? new ChargeResult(false, null, "card_declined")
                : new ChargeResult(true, "ch_" + UUID.randomUUID().toString().replace("-", ""), null));
        if (random.nextDouble() < c.timeoutRate()) {
            throw new GatewayTimeoutException("Payment gateway did not answer in time");
        }
        return result;
    }

    @Override
    public Optional<ChargeResult> lookup(String idempotencyKey) {
        return Optional.ofNullable(processed.get(idempotencyKey));
    }

    @Override
    public void refund(String gatewayRef) {
        sleep(config.get().latencyMs());
    }

    private static void sleep(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
