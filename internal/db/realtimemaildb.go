package db

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	ingestevents "github.com/alexis-dragneel/realtime-mail-agent/internal/server/models/ingest_events"
)

type DB interface {
	CreateEvents(context.Context, *ingestevents.IngestEvent, OutboxMapperFunc) error
}

type RealtimeMailDB struct {
	queries *realtimemailsql.Queries
	pool    DBX
}

func NewRealtimeMailDB(p DBX) DB {
	q := realtimemailsql.New(p)
	return &RealtimeMailDB{
		pool:    p,
		queries: q,
	}
}

func (r *RealtimeMailDB) CreateEvents(ctx context.Context, e *ingestevents.IngestEvent, maper OutboxMapperFunc) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	qTx := r.queries.WithTx(tx)
	incomingEventID, created, err := createIncomingEvent(ctx, qTx, e)
	if !created {
		return nil
	}
	if err != nil {
		return err
	}
	err = createOutbocEvent(ctx, qTx, maper(incomingEventID, e))
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
