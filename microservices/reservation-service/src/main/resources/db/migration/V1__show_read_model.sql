-- reservation-service's local copy of what it needs to know about shows. It is a read-model fed by
-- ShowPublished events from catalog-service (full snapshots, applied as upserts), never written by users.
-- The table names match the monolith's so the inventory and reservation SQL, and their foreign keys, are unchanged.
CREATE TABLE shows (
    id                 bigint      PRIMARY KEY,         -- the catalog's show id
    venue_id           bigint      NOT NULL,
    category           text        NOT NULL CHECK (category IN ('MOVIE', 'CONCERT')),
    starts_at          timestamptz NOT NULL,
    sale_opens_at      timestamptz NOT NULL,
    hold_ttl_sec       int         NOT NULL CHECK (hold_ttl_sec > 0),
    queue_enabled      boolean     NOT NULL,
    max_seats_per_hold int         NOT NULL CHECK (max_seats_per_hold > 0),
    max_holds_per_user int CHECK (max_holds_per_user > 0),   -- NULL = unlimited
    admit_rate_per_sec int         NOT NULL CHECK (admit_rate_per_sec > 0),
    ga_shards          int         NOT NULL DEFAULT 0 CHECK (ga_shards BETWEEN 0 AND 64),
    currency           text        NOT NULL,
    status             text        NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'CANCELLED')),
    inventory_ready    boolean     NOT NULL DEFAULT false,  -- true once show_seats / GA counters exist
    updated_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE show_tier_prices (
    show_id     bigint NOT NULL REFERENCES shows (id),
    tier_id     bigint NOT NULL,
    price_minor bigint NOT NULL CHECK (price_minor >= 0),
    PRIMARY KEY (show_id, tier_id)
);
