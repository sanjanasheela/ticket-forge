/**
 * Cross-cutting building blocks used by every module: ids, money, error model, clock.
 * Declared OPEN so other modules may use any of its types.
 */
@ApplicationModule(type = ApplicationModule.Type.OPEN)
package com.ticketforge.shared;

import org.springframework.modulith.ApplicationModule;
