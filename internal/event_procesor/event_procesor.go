package eventprocesor

import (
	"context"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
)

type processorDB interface {
	ClaimOutboxEvents(context.Context, db.ClaimOutboxEventsParams) ([]realtimemailsql.OutboxEvent, error)
}
