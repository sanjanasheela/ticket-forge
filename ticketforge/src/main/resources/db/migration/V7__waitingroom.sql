-- One global sequence: positions are not contiguous per show, but strictly increasing, which is
-- all FIFO needs. Avoids a per-show counter row that every joiner would fight over.
CREATE SEQUENCE queue_position_seq;

CREATE TABLE queue_entries (
    show_id     bigint      NOT NULL REFERENCES shows (id),
    user_id     uuid        NOT NULL,
    position    bigint      NOT NULL DEFAULT nextval('queue_position_seq'),
    joined_at   timestamptz NOT NULL,
    admitted_at timestamptz,
    PRIMARY KEY (show_id, user_id)
);
CREATE INDEX queue_waiting_idx ON queue_entries (show_id, position) WHERE admitted_at IS NULL;
