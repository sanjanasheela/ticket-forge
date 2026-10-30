CREATE TABLE holds (
    id          uuid PRIMARY KEY,
    user_id     uuid        NOT NULL ,
    show_id     bigint      NOT NULL REFERENCES shows (id),
    status      text        NOT NULL CHECK (status IN ('HELD', 'CONFIRMED', 'EXPIRED', 'RELEASED')),
    single_hold boolean     NOT NULL,   -- show allows one active hold per user
    expires_at  timestamptz NOT NULL,
    total_minor bigint      NOT NULL DEFAULT 0,
    currency    text        NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz
);
-- "One active hold per user per show" for concerts, enforced by the database, not by a racy count.
CREATE UNIQUE INDEX holds_single_active_uq ON holds (show_id, user_id) WHERE status = 'HELD' AND single_hold;
CREATE INDEX holds_expiry_idx ON holds (expires_at) WHERE status = 'HELD';
CREATE INDEX holds_user_idx ON holds (user_id, show_id) WHERE status = 'HELD';

CREATE TABLE hold_items (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hold_id       uuid   NOT NULL REFERENCES holds (id),
    show_id       bigint NOT NULL,
    tier_id       bigint NOT NULL,
    venue_seat_id bigint,          -- reserved seat; NULL for GA
    ga_shard      int,             -- GA only: which shard the units came from (NULL = main counter)
    quantity      int    NOT NULL CHECK (quantity > 0),
    price_minor   bigint NOT NULL,
    label         text   NOT NULL
);
CREATE INDEX hold_items_hold_idx ON hold_items (hold_id);
CREATE INDEX hold_items_seat_idx ON hold_items (show_id, venue_seat_id) WHERE venue_seat_id IS NOT NULL;
