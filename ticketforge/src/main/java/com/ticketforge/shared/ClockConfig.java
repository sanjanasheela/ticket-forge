package com.ticketforge.shared;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Single source of "now" for the whole app. Hold expiry, sweepers and SQL {@code :now}
 * parameters all read from this bean, so tests can swap in a fixed or stepping clock.
 */
@Configuration(proxyBeanMethods = false)
class ClockConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
