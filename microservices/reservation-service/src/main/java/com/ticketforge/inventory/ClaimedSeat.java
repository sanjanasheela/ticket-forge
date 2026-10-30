package com.ticketforge.inventory;

public record ClaimedSeat(long seatId, long tierId, String label) {
}
