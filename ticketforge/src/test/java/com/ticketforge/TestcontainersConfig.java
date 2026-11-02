package com.ticketforge;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Real Postgres for integration tests (same major version as docker-compose), plus a steerable clock. */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfig {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return new PostgreSQLContainer("postgres:16-alpine").withCommand("postgres", "-c", "max_connections=200");
    }

    @Bean
    @Primary
    TestClock testClock() {
        return new TestClock();
    }
}
