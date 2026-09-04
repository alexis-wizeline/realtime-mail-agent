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
         OR (
         status = 'processing'
         AND attempts < max_attempts
             AND
         locked_until IS NOT NULL
             AND locked_until <= Now()
            )
ORDER BY next_attempt_at, created_at
LIMIT $1::int
FOR UPDATE SKIP LOCKED;

-- name: ClaimOutboxEvents :exec
UPDATE outbox_events
SET locked_by = $1,
locked_until = $2,
attempts = attempts + 1,
status = 'processing',
updated_at = NOW()
FROM unnest($3::UUID[]) AS ids
WHERE outbox_events.id = ids;

-- name: MarkOutboxEventsAsFailed :exec
UPDATE outbox_events
SET status = 'failed',
last_error = $1,
next_attempt_at = $2,
updated_at = NOW(),
locked_by = NULL,
locked_until = NULL
FROM unnest($3::UUID[]) AS ids
WHERE outbox_events.id = ids;

-- name: MarkOutboxEventsAsPublished :exec
UPDATE outbox_events
SET status = 'published',
published_at = NOW(),
updated_at = NOW(),
locked_by = NULL,
locked_until = NULL
FROM unnest($1::UUID[]) AS ids
WHERE outbox_events.id = ids
AND status = 'processing';

-- name: SetProcessingIncomingEvent :exec
UPDATE incoming_events
SET status = $2,
updated_at = NOW()
FROM unnest($1::UUID[]) AS ids
WHERE incoming_events.id = ids;
