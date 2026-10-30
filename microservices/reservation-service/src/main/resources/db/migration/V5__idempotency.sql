-- Retried hold requests with the same Idempotency-Key replay the first response.
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
