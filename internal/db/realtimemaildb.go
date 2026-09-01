package db

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	ingestevents "github.com/alexis-dragneel/realtime-mail-agent/internal/server/models/ingest_events"
)

type DB interface {
	CreateEvents(context.Context, *ingestevents.IngestEvent) error
}

type RealtimeMailDB struct {
	queries           *realtimemailsql.Queries
	pool              DBX
	outboxEventMapper OutboxMapperFunc
}

func NewRealtimeMailDB(p DBX, mapper OutboxMapperFunc) DB {
	if mapper == nil {
		mapper = DefaultOutboxMapper
	}
	q := realtimemailsql.New(p)
	return &RealtimeMailDB{
		pool:              p,
		queries:           q,
		outboxEventMapper: mapper,
	}
}

func (r *RealtimeMailDB) CreateEvents(ctx context.Context, e *ingestevents.IngestEvent) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	qTx := r.queries.WithTx(tx)
	incomingEventID, created, err := createIncomingEvent(ctx, qTx, e)
	if err != nil {
		return err
	}
	if !created {
		return nil
	}
	err = createOutboxEvent(ctx, qTx, r.outboxEventMapper(incomingEventID, e))
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
