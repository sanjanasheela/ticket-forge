package com.ticketforge.catalog;

/**
 * Defaults per category (see spec §7.1). They are copied onto the show row at creation, so an
 * admin can override any of them per show.
 */
public enum ShowCategory {

    /** BookMyShow / Fandango: long enough to type a card and finish OTP / 3-D Secure. */
    MOVIE(420, false, 10, null, 200),

    /** Ticketmaster / flash sale: unpaid seats go back to the pool fast; queue on; anti-scalping. */
    CONCERT(180, true, 6, 1, 200);

    public final int holdTtlSec;
    public final boolean queueEnabled;
    public final int maxSeatsPerHold;
    public final Integer maxHoldsPerUser;
    public final int admitRatePerSec;

    ShowCategory(int holdTtlSec, boolean queueEnabled, int maxSeatsPerHold, Integer maxHoldsPerUser,
            int admitRatePerSec) {
        this.holdTtlSec = holdTtlSec;
        this.queueEnabled = queueEnabled;
        this.maxSeatsPerHold = maxSeatsPerHold;
        this.maxHoldsPerUser = maxHoldsPerUser;
        this.admitRatePerSec = admitRatePerSec;
    }
}
