package com.ticketforge.payment;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.PositiveOrZero;

/** The mock gateway's chaos knobs, changeable at runtime by an admin. */
public record GatewayConfig(@PositiveOrZero long latencyMs, @PositiveOrZero long jitterMs,
        @DecimalMin("0") @DecimalMax("1") double failureRate, @DecimalMin("0") @DecimalMax("1") double timeoutRate) {
}
