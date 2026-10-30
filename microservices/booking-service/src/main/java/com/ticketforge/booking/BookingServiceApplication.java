package com.ticketforge.booking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.ticketforge.security.ServiceRoutes;

/** Checkout, payments and the saga that ties a payment to a hold owned by reservation-service. */
@SpringBootApplication(scanBasePackages = "com.ticketforge")
@EnableScheduling
public class BookingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(BookingServiceApplication.class, args);
    }

    /** Only admins may touch the mock gateway's failure settings; everything else needs a login. */
    @Bean
    ServiceRoutes bookingRoutes() {
        return routes -> routes.requestMatchers("/admin/**").hasRole("ADMIN");
    }
}
