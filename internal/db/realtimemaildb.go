package db

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	ingestevents "github.com/alexis-dragneel/realtime-mail-agent/internal/server/models/ingest_events"
)

type Serializable[T any] struct {
	data T
	buf  []byte
}

type DB interface {
	CreateEvents(context.Context, *ingestevents.IngestEvent) error
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

func (r *RealtimeMailDB) CreateEvents(ctx context.Context, e *ingestevents.IngestEvent) error {

	buf, err := e.Serialize()
	if err != nil {
		return err
	}

	ser := Serializable[*ingestevents.IngestEvent]{data: e, buf: buf}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	qTx := r.queries.WithTx(tx)
	incomingEventID, err := createIncomingEvent(ctx, qTx, ser)
	if err != nil {
		if errors.Is(err, dupliateIncominEventErr) {
			return nil
		}
		return err
	}
	err = createOutbocEvent(ctx, qTx,
		createOutboxEventParams{
			incommingEventID: incomingEventID,
			payload:          ser,
		})
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
