-- booking_db: money and the saga that ties a payment to a hold held by another service.
-- No foreign keys to holds: that table belongs to reservation-service. hold_id is a plain reference.

CREATE TABLE payments (
    id             uuid PRIMARY KEY,
    hold_id        uuid        NOT NULL,
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
    hold_id     uuid        NOT NULL UNIQUE,
    user_id     uuid        NOT NULL,
    show_id     bigint      NOT NULL,
    payment_id  uuid        NOT NULL REFERENCES payments (id),
    status      text        NOT NULL CHECK (status IN ('CONFIRMED', 'REFUNDED')),
    total_minor bigint      NOT NULL,
    currency    text        NOT NULL,
    items       text        NOT NULL,   -- JSON copy of what was bought, so listing bookings never calls another service
    created_at  timestamptz NOT NULL
);
CREATE INDEX bookings_user_idx ON bookings (user_id, created_at DESC);

-- One saga per hold. Its state is the single source of truth for where a checkout is, so a crash at
-- any step resumes from the table instead of losing money.
--   CHARGING        payment row created, gateway call in flight
--   CHARGE_UNKNOWN  gateway timed out; the reconciler asks the gateway what happened
--   CHARGED         money taken, ConfirmHold sent, waiting for reservation-service's reply
--   BOOKED          done: booking written                               (terminal)
--   REFUNDING       the hold could not be confirmed; refund in progress
--   REFUNDED        money returned                                      (terminal)
--   FAILED          card declined; the hold is untouched and the buyer may try again
CREATE TABLE sagas (
    id           uuid PRIMARY KEY,
    hold_id      uuid        NOT NULL UNIQUE,
    user_id      uuid        NOT NULL,
    show_id      bigint      NOT NULL,
    total_minor  bigint      NOT NULL,
    currency     text        NOT NULL,
    items        text        NOT NULL,
    payment_id   uuid,
    gateway_ref  text,
    state        text        NOT NULL CHECK (state IN ('CHARGING', 'CHARGE_UNKNOWN', 'CHARGED', 'BOOKED', 'REFUNDING', 'REFUNDED', 'FAILED')),
    booking_id   uuid,
    reason       text,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL
);
CREATE INDEX sagas_stuck_idx ON sagas (updated_at) WHERE state IN ('CHARGED', 'REFUNDING');
