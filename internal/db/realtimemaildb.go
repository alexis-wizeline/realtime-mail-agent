package db

import (
	"context"
	"encoding/json"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/server/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type DB interface {
	CreateEvents(context.Context, *models.IngestEvent) error
}

type DBX interface {
	realtimemailsql.DBTX

	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Ping(context.Context) error
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

func (r *RealtimeMailDB) CreateEvents(ctx context.Context, e *models.IngestEvent) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	qTx := r.queries.WithTx(tx)
	incomingUUID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	buf, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = qTx.CreateIncomingEvent(ctx, realtimemailsql.CreateIncomingEventParams{
		ID: pgtype.UUID{
			Bytes: incomingUUID,
			Valid: true,
		},
		EventID:   e.EventID,
		UserID:    e.UserID,
		MessageID: e.MessageID,
		EventType: e.Type,
		Payload:   buf,
	})
	if err != nil {
		return err
	}
	outboxUUID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	_, err = qTx.CreateOutboxEvent(ctx, realtimemailsql.CreateOutboxEventParams{
		ID: pgtype.UUID{
			Bytes: outboxUUID,
			Valid: true,
		},
		IncomingEventID: pgtype.UUID{
			Bytes: incomingUUID,
			Valid: true,
		},
		Topic:   "events",
		Payload: buf,
	})
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
