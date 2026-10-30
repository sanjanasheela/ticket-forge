package com.ticketforge.auth;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Identity: signup, login, refresh-token rotation. Issues the JWTs every other service verifies locally. */
@SpringBootApplication(scanBasePackages = "com.ticketforge")
public class AuthServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(AuthServiceApplication.class, args);
    }
}
