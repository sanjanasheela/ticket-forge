package com.ticketforge.messaging;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import forgelog.Client;
import forgelog.GroupConsumer;
import forgelog.Producer;

/** {@link MessageBus} on the custom forgelog broker (Phase 3C). */
@Component
@ConditionalOnProperty(name = "ticketforge.bus.type", havingValue = "forgelog")
class ForgeLogMessageBus implements MessageBus, DisposableBean {

    private final String bootstrap;
    private final Producer producer;

    ForgeLogMessageBus(@Value("${ticketforge.bus.forgelog.bootstrap:localhost:9192}") String bootstrap) {
        this.bootstrap = bootstrap;
        this.producer = new Producer(bootstrap);
    }

    @Override
    public CompletableFuture<?> send(String topic, String key, String type, String eventId, String payload) {
        return producer.send(new Client.Msg(topic, key, type, eventId, payload));
    }

    @Override
    public AutoCloseable subscribe(String group, String topic, Handler handler) {
        GroupConsumer consumer = new GroupConsumer(bootstrap, group, topic, record -> handler.handle(record.valueString()));
        return consumer::close;
    }

    @Override
    public void destroy() {
        producer.close();
    }
}
