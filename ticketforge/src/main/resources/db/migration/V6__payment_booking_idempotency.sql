CREATE TABLE payments (
    id             uuid PRIMARY KEY,
    hold_id        uuid        NOT NULL REFERENCES holds (id),
    user_id        uuid        NOT NULL,
    amount_minor   bigint      NOT NULL,
    currency       text        NOT NULL,
    status         text        NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'REFUNDED')),
    gateway_ref    text,
    failure_reason text,
    created_at     timestamptz NOT NULL,
    updated_at     timestamptz
);
-- Two concurrent checkouts of one hold (different idempotency keys, two tabs) cannot both charge.
CREATE UNIQUE INDEX payments_one_live_per_hold ON payments (hold_id) WHERE status IN ('PENDING', 'SUCCEEDED');
CREATE INDEX payments_pending_idx ON payments (created_at) WHERE status = 'PENDING';

CREATE TABLE bookings (
    id          uuid PRIMARY KEY,
    hold_id     uuid        NOT NULL UNIQUE REFERENCES holds (id),
    user_id     uuid        NOT NULL,
    show_id     bigint      NOT NULL,
    payment_id  uuid        NOT NULL REFERENCES payments (id),
    status      text        NOT NULL CHECK (status IN ('CONFIRMED', 'REFUNDED')),
    total_minor bigint      NOT NULL,
    currency    text        NOT NULL,
    created_at  timestamptz NOT NULL
);
CREATE INDEX bookings_user_idx ON bookings (user_id, created_at DESC);

CREATE TABLE idempotency_keys (
    scope         text        NOT NULL,   -- user id, or 'anonymous' for signup
    idem_key      text        NOT NULL,
    request_hash  text        NOT NULL,
    status        text        NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED')),
    response_code int,
    response_body text,
    content_type  text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, idem_key)
);
CREATE INDEX idempotency_keys_created_idx ON idempotency_keys (created_at);
