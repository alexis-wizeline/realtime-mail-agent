package processors

import (
	"context"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
)

type Processor interface {
	Process(context.Context, realtimemailsql.OutboxEvent) error
}
