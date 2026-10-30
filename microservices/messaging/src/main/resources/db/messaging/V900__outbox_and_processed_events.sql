-- Shipped by the messaging module; every service that uses events gets these two tables in its own database.

-- Transactional outbox: the event row is written in the same transaction as the state change, so
-- "database committed but event lost" cannot happen. The relay publishes rows in id order.
CREATE TABLE outbox (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic      text        NOT NULL,
    msg_key    text        NOT NULL,   -- Kafka partition key: events of one aggregate stay ordered
    type       text        NOT NULL,   -- event type, sent as a header
    payload    text        NOT NULL,   -- JSON
    created_at timestamptz NOT NULL DEFAULT now(),
    sent_at    timestamptz
);
CREATE INDEX outbox_unsent_idx ON outbox (id) WHERE sent_at IS NULL;

-- Kafka delivers at least once, so consumers remember what they already handled.
CREATE TABLE processed_events (
    consumer     text        NOT NULL,
    event_id     text        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
