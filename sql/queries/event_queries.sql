-- name: CreateIncommingEvent :one
INSERT INTO incomming_events (
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
incomming_event_id,
topic,
payload
) VALUES (
$1, $2, $3, $4
) ON CONFLICT (incomming_event_id) DO NOTHING RETURNING *;
