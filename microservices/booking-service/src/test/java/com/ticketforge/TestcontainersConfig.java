package com.ticketforge;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Real Postgres and Kafka, and a stand-in for reservation-service (the same images the stack uses), plus a steerable clock. */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfig {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return new PostgreSQLContainer("postgres:16-alpine").withCommand("postgres", "-c", "max_connections=200");
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        return new KafkaContainer("apache/kafka:3.8.0");
    }

    @Bean(destroyMethod = "close")
    StubReservation stubReservation(KafkaContainer kafka) {
        return new StubReservation(kafka.getBootstrapServers());
    }

    @Bean
    DynamicPropertyRegistrar reservationUrl(StubReservation stub) {
        return registry -> registry.add("ticketforge.reservation.url", stub::url);
    }

    @Bean
    @Primary
    TestClock testClock() {
        return new TestClock();
    }
}
