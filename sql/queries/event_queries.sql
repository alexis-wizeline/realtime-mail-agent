-- name: CreateIncomingEvent :one
INSERT INTO incoming_events (
id,
event_id,
event_type,
user_id,
message_id,
payload
)
VALUES (
$1, $2, $3, $4, $5, $6
) ON CONFLICT (event_id) DO NOTHING RETURNING *;

-- name: CreateOutboxEvent :one
INSERT INTO outbox_events (
id,
incoming_event_id,
topic,
event_type,
payload
) VALUES (
$1, $2, $3, $4, $5
) ON CONFLICT (incoming_event_id, event_type) DO NOTHING RETURNING *;
