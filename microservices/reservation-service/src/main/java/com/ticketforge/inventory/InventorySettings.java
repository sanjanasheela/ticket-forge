package com.ticketforge.inventory;

import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Runtime-switchable knobs for experiments (admin endpoint), seeded from configuration. */
@Component
class InventorySettings {

    private final AtomicReference<LockStrategy> strategy;
    private volatile boolean gapRuleEnabled;

    InventorySettings(@Value("${ticketforge.inventory.strategy}") LockStrategy strategy,
            @Value("${ticketforge.inventory.gap-rule-enabled}") boolean gapRuleEnabled) {
        this.strategy = new AtomicReference<>(strategy);
        this.gapRuleEnabled = gapRuleEnabled;
    }

    LockStrategy strategy() {
        return strategy.get();
    }

    void strategy(LockStrategy s) {
        strategy.set(s);
    }

    boolean gapRuleEnabled() {
        return gapRuleEnabled;
    }

    void gapRuleEnabled(boolean enabled) {
        this.gapRuleEnabled = enabled;
    }
}
