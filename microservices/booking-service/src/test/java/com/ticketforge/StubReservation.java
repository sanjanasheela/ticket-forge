package com.ticketforge;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.sun.net.httpserver.HttpServer;
import com.ticketforge.contracts.SagaMessages.ConfirmHold;
import com.ticketforge.contracts.SagaMessages.ConfirmHoldReply;
import com.ticketforge.contracts.SagaMessages.HoldInfo;
import com.ticketforge.contracts.Topics;

import tools.jackson.databind.json.JsonMapper;

/**
 * Stands in for reservation-service on both of its saga interfaces: the HTTP hold lookup
 * ({@code /internal/holds/{id}}) and the Kafka command handler (ConfirmHold -> reply). Tests choose, per hold,
 * whether the command is confirmed, rejected or ignored, and can take the HTTP side "down".
 */
public class StubReservation implements AutoCloseable {

    public enum OnConfirm { CONFIRM, REJECT, SILENT }

    private final JsonMapper json = JsonMapper.builder().findAndAddModules().build();
    private final HttpServer server;
    private final KafkaProducer<String, String> producer;
    private final Thread poller;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final Map<UUID, HoldInfo> holds = new ConcurrentHashMap<>();
    private final Map<UUID, OnConfirm> behaviour = new ConcurrentHashMap<>();
    private final List<ConfirmHold> commands = new CopyOnWriteArrayList<>();
    private final AtomicBoolean down = new AtomicBoolean();

    StubReservation(String bootstrap) {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        server.createContext("/internal/holds/", exchange -> {
            UUID holdId = UUID.fromString(exchange.getRequestURI().getPath().substring("/internal/holds/".length()));
            HoldInfo hold = holds.get(holdId);
            int status = down.get() ? 503 : hold == null ? 404 : hold.expiresAt().isBefore(java.time.Instant.now()) ? 409 : 200;
            byte[] body = status == 200 ? json.writeValueAsString(hold).getBytes(StandardCharsets.UTF_8) : new byte[0];
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
            if (body.length > 0) {
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        });
        server.start();

        producer = new KafkaProducer<>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        poller = Thread.ofPlatform().daemon().name("stub-reservation").start(() -> {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                    ConsumerConfig.GROUP_ID_CONFIG, "stub-reservation-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
                consumer.subscribe(List.of(Topics.SAGA_RESERVATION_COMMANDS));
                while (running.get()) {
                    for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(100))) {
                        handle(json.readValue(r.value(), ConfirmHold.class));
                    }
                }
            }
        });
    }

    private void handle(ConfirmHold cmd) {
        commands.add(cmd);
        OnConfirm what = behaviour.getOrDefault(cmd.holdId(), OnConfirm.CONFIRM);
        if (what == OnConfirm.SILENT) {
            return;
        }
        reply(new ConfirmHoldReply(cmd.sagaId(), cmd.holdId(), what == OnConfirm.CONFIRM,
                what == OnConfirm.CONFIRM ? null : "HOLD_EXPIRED"));
    }

    /** Publishes a reply, also used by tests to inject duplicates. */
    public void reply(ConfirmHoldReply reply) {
        ProducerRecord<String, String> record = new ProducerRecord<>(Topics.SAGA_RESERVATION_REPLIES,
                reply.holdId().toString(), json.writeValueAsString(reply));
        record.headers().add(new RecordHeader("type", Topics.CONFIRM_HOLD_REPLY.getBytes(StandardCharsets.UTF_8)));
        producer.send(record);
        producer.flush();
    }

    public String url() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    public void registerHold(HoldInfo hold) {
        holds.put(hold.holdId(), hold);
    }

    public void onConfirm(UUID holdId, OnConfirm what) {
        behaviour.put(holdId, what);
    }

    public long commandsFor(UUID holdId) {
        return commands.stream().filter(c -> c.holdId().equals(holdId)).count();
    }

    public void setDown(boolean value) {
        down.set(value);
    }

    @Override
    public void close() {
        running.set(false);
        server.stop(0);
        producer.close(Duration.ofSeconds(2));
    }
}
