package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	ingestevents "github.com/alexis-dragneel/realtime-mail-agent/internal/server/models/ingest_events"
)

type DB interface {
	CreateEvents(context.Context, *ingestevents.IngestEvent) error
	ClaimOutboxEvents(context.Context, ClaimOutboxEventsParams) ([]realtimemailsql.OutboxEvent, error)
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

type ClaimOutboxEventsParams struct {
	WorkerName  string
	LockedUntil time.Time

	JobsLimit int32
}

func (r *RealtimeMailDB) ClaimOutboxEvents(ctx context.Context, p ClaimOutboxEventsParams) ([]realtimemailsql.OutboxEvent, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	qTx := r.queries.WithTx(tx)

	jobsLimt := p.JobsLimit
	if jobsLimt == 0 {
		jobsLimt = DEFAULT_JOBS_LIMIT
	}

	jobs, err := qTx.GetOutboxEventsToProcess(ctx, jobsLimt)
	if err != nil {
		return nil, &DbQueryError{
			QueryName: "GetOutboxEventsToProcess",
			Err:       err,
		}
	}
	jobIDs := outboxJobsIDs(jobs)
	err = qTx.ClaimOutboxEvents(ctx, realtimemailsql.ClaimOutboxEventsParams{
		LockedBy: pgtype.Text{
			String: p.WorkerName,
			Valid:  len(p.WorkerName) > 0,
		},
		LockedUntil: pgtype.Timestamptz{
			Time:  p.LockedUntil,
			Valid: true,
		},
		OutboxEventIds: jobIDs,
	})
	if err != nil {
		return nil, &DbQueryError{
			QueryName: "ClaimOutboxEvents",
			Err:       err,
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		return nil, err
	}

	return jobs, nil
}
