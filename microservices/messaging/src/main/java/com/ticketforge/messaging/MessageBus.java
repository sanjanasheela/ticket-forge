package com.ticketforge.messaging;

import java.util.concurrent.CompletableFuture;

/**
 * The only way services talk to the message broker. Phase 2 implements it on Kafka, Phase 3 also on the custom
 * {@code forgelog} broker; {@code ticketforge.bus.type} picks one. Delivery is at least once, order is kept per key.
 */
public interface MessageBus {

    /** Handles one message payload. Throwing makes the bus retry the same message after a pause; nothing is skipped. */
    @FunctionalInterface
    interface Handler {
        void handle(String payload) throws Exception;
    }

    /** Sends one message; the future completes when the broker has accepted it. */
    CompletableFuture<?> send(String topic, String key, String type, String eventId, String payload);

    /** Starts consuming {@code topic} as part of consumer group {@code group}. */
    AutoCloseable subscribe(String group, String topic, Handler handler);
}
