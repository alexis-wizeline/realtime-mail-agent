package eventprocesor

import (
	"context"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	"github.com/google/uuid"
)

type processorDB interface {
	ClaimOutboxEvents(context.Context, db.ClaimOutboxEventsParams) ([]realtimemailsql.OutboxEvent, error)
	MarkOutboxEventAsPublished(context.Context, uuid.UUID, uuid.UUID) (bool, error)
	MarkOutboxEventAsFailed(context.Context, db.FailedEventParams) (bool, error)
	MarkOutboxEventAsDiscarded(context.Context, db.FailedEventParams) (bool, error)
}
