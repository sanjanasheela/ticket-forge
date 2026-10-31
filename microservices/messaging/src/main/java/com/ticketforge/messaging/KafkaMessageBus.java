package com.ticketforge.messaging;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

/** {@link MessageBus} on Apache Kafka (the Phase 2 default). */
@Component
@ConditionalOnProperty(name = "ticketforge.bus.type", havingValue = "kafka", matchIfMissing = true)
class KafkaMessageBus implements MessageBus {

    private final KafkaTemplate<String, String> kafka;
    private final ConsumerFactory<String, String> consumers;
    private final int concurrency;

    KafkaMessageBus(KafkaTemplate<String, String> kafka, ConsumerFactory<String, String> consumers,
            @Value("${spring.kafka.listener.concurrency:1}") int concurrency) {
        this.kafka = kafka;
        this.consumers = consumers;
        this.concurrency = concurrency;
    }

    @Override
    public CompletableFuture<?> send(String topic, String key, String type, String eventId, String payload) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, payload);
        record.headers().add(new RecordHeader("type", type.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("eventId", eventId.getBytes(StandardCharsets.UTF_8)));
        return kafka.send(record);
    }

    @Override
    public AutoCloseable subscribe(String group, String topic, Handler handler) {
        ContainerProperties props = new ContainerProperties(topic);
        props.setGroupId(group);
        props.setMessageListener((org.springframework.kafka.listener.MessageListener<String, String>) record -> {
            try {
                handler.handle(record.value());
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        ConcurrentMessageListenerContainer<String, String> container = new ConcurrentMessageListenerContainer<>(consumers, props);
        container.setConcurrency(concurrency);
        // A message must not be skipped: retry forever, one second apart, instead of Boot's default of dropping it after 9 tries.
        container.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1_000, FixedBackOff.UNLIMITED_ATTEMPTS)));
        container.start();
        return container::stop;
    }
}
