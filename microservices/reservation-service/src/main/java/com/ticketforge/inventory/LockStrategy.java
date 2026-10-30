package com.ticketforge.inventory;

/**
 * How a claim takes seats (and single-row GA units). Switchable at runtime so one k6 run can
 * compare them. See spec §6.
 */
public enum LockStrategy {

    /** SELECT ... FOR UPDATE in seat-id order, check, then UPDATE. Waits on row locks. */
    PESSIMISTIC,

    /** Read with version, UPDATE ... WHERE version = :v; a lost race is retried. */
    OPTIMISTIC,

    /** One conditional UPDATE whose WHERE clause is the availability check. */
    ATOMIC_CONDITIONAL
}
