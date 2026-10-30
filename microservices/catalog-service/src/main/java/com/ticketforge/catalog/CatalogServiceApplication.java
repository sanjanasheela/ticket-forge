package com.ticketforge.catalog;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Venues, events and shows. The read-heavy edge of the system, fronted by a Redis cache. */
@SpringBootApplication(scanBasePackages = "com.ticketforge")
public class CatalogServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(CatalogServiceApplication.class, args);
    }
}
