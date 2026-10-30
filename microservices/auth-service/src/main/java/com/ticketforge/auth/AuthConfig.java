package com.ticketforge.auth;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import com.ticketforge.security.ServiceRoutes;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(AuthServiceProperties.class)
class AuthConfig {

    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder(10);
    }

    /** Login and signup are public; everything under /admin needs the ADMIN role. */
    @Bean
    ServiceRoutes authRoutes() {
        return routes -> routes
                .requestMatchers("/auth/**").permitAll()
                .requestMatchers("/admin/**").hasRole("ADMIN");
    }
}
