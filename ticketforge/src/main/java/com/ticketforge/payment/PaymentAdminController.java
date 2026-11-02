package com.ticketforge.payment;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;

/** Phase 1's chaos knob: make payments slow, flaky, or lose their responses. */
@RestController
@RequestMapping("/admin/payment-gateway/config")
class PaymentAdminController {

    private final MockPaymentGateway gateway;

    PaymentAdminController(MockPaymentGateway gateway) {
        this.gateway = gateway;
    }

    @GetMapping
    GatewayConfig get() {
        return gateway.config();
    }

    @PutMapping
    GatewayConfig put(@Valid @RequestBody GatewayConfig config) {
        gateway.config(config);
        return config;
    }
}
