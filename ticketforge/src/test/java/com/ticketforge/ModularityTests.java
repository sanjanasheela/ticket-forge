package com.ticketforge;

import org.junit.jupiter.api.Test;
import org.springframework.modulith.core.ApplicationModules;

/**
 * Fails the build if any module reaches into another module's internals.
 * These are the seams Phase 2 splits into services along.
 */
class ModularityTests {

    @Test
    void verifiesModuleBoundaries() {
        ApplicationModules.of(TicketForgeApplication.class).verify();
    }
}
