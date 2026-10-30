package com.ticketforge;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Real Postgres and Kafka (the same images the stack uses), plus a steerable clock. */
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
    StubCatalog stubCatalog() {
        return new StubCatalog();
    }

    @Bean
    DynamicPropertyRegistrar catalogUrl(StubCatalog stub) {
        return registry -> registry.add("ticketforge.catalog.url", stub::url);
    }

    @Bean
    @Primary
    TestClock testClock() {
        return new TestClock();
    }
}
