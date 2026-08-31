-- +goose Up
CREATE TABLE outbox_events(
    id UUID PRIMARY KEY,

    incoming_event_id UUID NOT NULL REFERENCES incoming_events(id),

    topic VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,

    status VARCHAR(255) NOT NULL DEFAULT 'pending',

    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,

    next_attempt_at TIMESTAMPTZ NULL,

    locked_by VARCHAR(255) NULL,
    locked_until TIMESTAMPTZ NULL,

    last_error TEXT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ NULL,

    CONSTRAINT outbox_events_status_check CHECK (status IN ('pending', 'processing', 'published', 'failed')),
    CONSTRAINT outbox_check_events_attempts CHECK (attempts >= 0),
    CONSTRAINT outbox_event_max_attempts_check CHECK (max_attempts > 0)
);

CREATE INDEX outbox_events_claims_idx ON outbox_events (status, next_attempt_at, locked_until);
CREATE INDEX outbox_events_incoming_event_idx ON outbox_events (incoming_event_id);

-- +goose Down
DROP TABLE outbox_events;
