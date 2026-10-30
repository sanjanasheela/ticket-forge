CREATE TABLE venues (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text        NOT NULL,
    city       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE seat_tiers (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    venue_id    bigint NOT NULL REFERENCES venues (id),
    name        text   NOT NULL,
    kind        text   NOT NULL CHECK (kind IN ('RESERVED', 'GA')),
    ga_capacity int,
    UNIQUE (venue_id, name),
    CHECK ((kind = 'GA') = (ga_capacity IS NOT NULL AND ga_capacity > 0))
);

CREATE TABLE venue_seats (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    venue_id    bigint  NOT NULL REFERENCES venues (id),
    tier_id     bigint  NOT NULL REFERENCES seat_tiers (id),
    section     text    NOT NULL,
    row_label   text    NOT NULL,
    row_order   int     NOT NULL,
    seat_number int     NOT NULL,
    aisle_left  boolean NOT NULL DEFAULT false,
    aisle_right boolean NOT NULL DEFAULT false,
    UNIQUE (venue_id, section, row_label, seat_number)
);

CREATE TABLE events (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    title        text        NOT NULL,
    genre        text        NOT NULL,
    duration_min int         NOT NULL CHECK (duration_min > 0),
    category     text        NOT NULL CHECK (category IN ('MOVIE', 'CONCERT')),
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE shows (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id           bigint      NOT NULL REFERENCES events (id),
    venue_id           bigint      NOT NULL REFERENCES venues (id),
    starts_at          timestamptz NOT NULL,
    sale_opens_at      timestamptz NOT NULL,
    hold_ttl_sec       int         NOT NULL CHECK (hold_ttl_sec > 0),
    queue_enabled      boolean     NOT NULL,
    max_seats_per_hold int         NOT NULL CHECK (max_seats_per_hold > 0),
    max_holds_per_user int CHECK (max_holds_per_user > 0),  -- NULL = unlimited
    admit_rate_per_sec int         NOT NULL CHECK (admit_rate_per_sec > 0),
    ga_shards          int         NOT NULL DEFAULT 0 CHECK (ga_shards BETWEEN 0 AND 64),
    currency           text        NOT NULL,
    status             text        NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'CANCELLED')),
    created_at         timestamptz NOT NULL DEFAULT now(),
    CHECK (sale_opens_at <= starts_at)
);
CREATE INDEX shows_event_idx ON shows (event_id, starts_at);

CREATE TABLE show_tier_prices (
    show_id     bigint NOT NULL REFERENCES shows (id),
    tier_id     bigint NOT NULL REFERENCES seat_tiers (id),
    price_minor bigint NOT NULL CHECK (price_minor >= 0),
    PRIMARY KEY (show_id, tier_id)
);
