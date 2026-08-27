-- +goose Up
CREATE TABLE incomming_events(
    id UUID PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    message_id VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,

    status VARCHAR(255) NOT NULL DEFAULT 'received',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,

    CONSTRAINT unique_event_id UNIQUE (event_id),
    CONSTRAINT status_check CHECK (status IN ('received', 'processing', 'processed', 'failed'))
);

-- +goose Down
DROP TABLE incomming_events;
