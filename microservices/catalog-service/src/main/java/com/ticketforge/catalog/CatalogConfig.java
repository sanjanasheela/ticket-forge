package com.ticketforge.catalog;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;

import com.ticketforge.security.ServiceRoutes;

@Configuration(proxyBeanMethods = false)
class CatalogConfig {

    /** Browsing is public; creating venues, events and shows needs the ADMIN role. */
    @Bean
    ServiceRoutes catalogRoutes() {
        return routes -> routes
                .requestMatchers(HttpMethod.GET, "/events/**", "/shows/*").permitAll()
                .requestMatchers("/admin/**").hasRole("ADMIN");
    }
}
