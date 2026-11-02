-- Per-show seat inventory. Layout columns are copied from venue_seats when the show is created
-- (a snapshot, like real ticketing systems keep), so the hot path never joins catalog tables.
CREATE TABLE show_seats (
    show_id       bigint  NOT NULL REFERENCES shows (id),
    venue_seat_id bigint  NOT NULL,
    tier_id       bigint  NOT NULL,
    section       text    NOT NULL,
    row_label     text    NOT NULL,
    row_order     int     NOT NULL,
    seat_number   int     NOT NULL,
    aisle_left    boolean NOT NULL,
    aisle_right   boolean NOT NULL,
    status        text    NOT NULL DEFAULT 'AVAILABLE' CHECK (status IN ('AVAILABLE', 'HELD', 'BOOKED')),
    hold_id       uuid,
    held_until    timestamptz,
    version       bigint  NOT NULL DEFAULT 0,
    PRIMARY KEY (show_id, venue_seat_id),
    CHECK ((status = 'AVAILABLE') = (hold_id IS NULL)),
    CHECK ((status = 'HELD') = (held_until IS NOT NULL))
);
CREATE INDEX show_seats_hold_idx ON show_seats (hold_id) WHERE hold_id IS NOT NULL;
CREATE INDEX show_seats_best_available_idx ON show_seats (show_id, tier_id, venue_seat_id) WHERE status <> 'BOOKED';
CREATE INDEX show_seats_row_idx ON show_seats (show_id, section, row_label);

-- General admission: one hot counter row per (show, tier). The CHECK is the last line of defence
-- against overselling, whatever the application does.
CREATE TABLE ga_inventory (
    show_id  bigint NOT NULL REFERENCES shows (id),
    tier_id  bigint NOT NULL,
    capacity int    NOT NULL,
    held     int    NOT NULL DEFAULT 0,
    sold     int    NOT NULL DEFAULT 0,
    version  bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (show_id, tier_id),
    CHECK (held >= 0 AND sold >= 0 AND held + sold <= capacity)
);

-- Used instead of ga_inventory.held/sold when the show has ga_shards > 0: the capacity is split
-- across K rows so concurrent buyers spread their row locks.
CREATE TABLE ga_inventory_shards (
    show_id  bigint NOT NULL REFERENCES shows (id),
    tier_id  bigint NOT NULL,
    shard    int    NOT NULL,
    capacity int    NOT NULL,
    held     int    NOT NULL DEFAULT 0,
    sold     int    NOT NULL DEFAULT 0,
    PRIMARY KEY (show_id, tier_id, shard),
    CHECK (held >= 0 AND sold >= 0 AND held + sold <= capacity)
);
