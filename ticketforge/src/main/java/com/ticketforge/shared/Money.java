package com.ticketforge.shared;

/** Amounts are always integer minor units (paise/cents) to avoid floating-point money. */
public record Money(long minor, String currency) {
}
