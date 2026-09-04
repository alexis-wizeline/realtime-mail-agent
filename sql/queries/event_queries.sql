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
) RETURNING *;


-- name: GetOutboxEventsToProcess :many
SELECT   *
FROM     outbox_events
WHERE    status = 'pending'
         OR (status = 'failed'
             AND attempts < max_attempts
             AND next_attempt_at <= Now())
         OR (locked_until IS NOT NULL
             AND locked_until <= Now())
ORDER BY created_at,
         next_attempt_at LIMIT $1::int
FOR UPDATE SKIP LOCKED;


-- name: UpdateLockedOutboxEvents :exec
UPDATE outbox_events
SET locked_by = $1,
locked_until = $2,
attempts = $3,
status = $4,
updated_at = NOW()
FROM unnest($5::UUID[]) AS ids
WHERE outbox_events.id = ids;

-- name: SetProcessingIncomingEvent :exec
UPDATE incoming_events
SET status = $2,
updated_at = NOW()
FROM unnest($1::UUID[]) AS ids
WHERE incoming_events.id = ids;
