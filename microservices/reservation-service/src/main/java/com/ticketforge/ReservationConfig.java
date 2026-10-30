package com.ticketforge;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;

import com.ticketforge.security.ServiceRoutes;

@Configuration(proxyBeanMethods = false)
class ReservationConfig {

    /** Seat maps and their live stream are public; holds, queue and the rest need a login. */
    @Bean
    ServiceRoutes reservationRoutes() {
        return routes -> routes
                .requestMatchers(HttpMethod.GET, "/shows/*/seats", "/shows/*/seats/stream").permitAll()
                .requestMatchers("/admin/**").hasRole("ADMIN");
    }
}
