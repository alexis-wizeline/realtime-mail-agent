package db

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	ingestevents "github.com/alexis-dragneel/realtime-mail-agent/internal/server/models/ingest_events"
)

type DB interface {
	CreateEvents(context.Context, *ingestevents.IngestEvent) error
	ClaimOutboxEvents(context.Context, ClaimOutboxEventsParams) ([]realtimemailsql.OutboxEvent, error)
	MarkOutboxEventAsPublished(context.Context, uuid.UUID, uuid.UUID) (bool, error)
	MarkOutboxEventAsFailed(context.Context, FailedEventParams) (bool, error)
	MarkOutboxEventAsDiscarded(context.Context, DiscardedEventParams) (bool, error)
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

func (c ClaimOutboxEventsParams) Valid() error {
	if len(c.WorkerName) == 0 {
		return EmptyWorkerName
	}
	if c.LockedUntil.IsZero() {
		return LockedTimeIsZero
	}

	return nil
}

func (r *RealtimeMailDB) ClaimOutboxEvents(ctx context.Context, p ClaimOutboxEventsParams) ([]realtimemailsql.OutboxEvent, error) {
	err := p.Valid()
	if err != nil {
		return nil, err
	}
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

	jobIDs, err := qTx.GetOutboxEventsToProcess(ctx, jobsLimt)
	if err != nil {
		return nil, &DbQueryError{
			QueryName: "GetOutboxEventsToProcess",
			Err:       err,
		}
	}
	jobs, err := qTx.ClaimOutboxEvents(ctx, realtimemailsql.ClaimOutboxEventsParams{
		LockedBy: pgtype.Text{
			String: p.WorkerName,
			Valid:  true,
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

func (r *RealtimeMailDB) MarkOutboxEventAsPublished(ctx context.Context, eventID uuid.UUID, workerID uuid.UUID) (bool, error) {
	if err := uuid.Validate(eventID.String()); err != nil {
		return false, fmt.Errorf("Inavlid EventID: %s", err.Error())
	}

	rows, err := r.queries.MarkOutboxEventsAsPublished(ctx, realtimemailsql.MarkOutboxEventsAsPublishedParams{
		LockedBy: pgtype.Text{
			String: workerID.String(),
			Valid:  true,
		},
		OutboxEventIds: []pgtype.UUID{
			{
				Bytes: eventID,
				Valid: true,
			},
		},
	})
	if err != nil {
		return false, &DbQueryError{
			QueryName: "MarkOutboxEventsAsPublished",
			Err:       err,
		}
	}

	return rows == 1, nil
}

type FailedEventParams struct {
	EventID       uuid.UUID
	WorkerID      uuid.UUID
	Err           error
	NextAttemptAt time.Time
}

func (m *FailedEventParams) valid() error {
	if m.Err == nil {
		return NilEventErr
	}
	if m.NextAttemptAt.Before(time.Now()) {
		return NextAttemptInThePast
	}
	return nil
}

func (r *RealtimeMailDB) MarkOutboxEventAsFailed(ctx context.Context, p FailedEventParams) (bool, error) {
	err := p.valid()
	if err != nil {
		return false, err
	}

	rows, err := r.queries.MarkOutboxEventsAsFailed(ctx, realtimemailsql.MarkOutboxEventsAsFailedParams{
		LastError: pgtype.Text{
			String: p.Err.Error(),
			Valid:  true,
		},
		NextAttemptAt: pgtype.Timestamptz{
			Time:  p.NextAttemptAt,
			Valid: true,
		},
		LockedBy: pgtype.Text{
			String: p.WorkerID.String(),
			Valid:  true,
		},
		OutboxEventIds: []pgtype.UUID{
			{
				Bytes: p.EventID,
				Valid: true,
			},
		},
	})
	if err != nil {
		return false, &DbQueryError{
			QueryName: "MarkOutboxEventsAsFailed",
			Err:       err,
		}
	}

	return rows == 1, nil
}

type DiscardedEventParams struct {
	EventID  uuid.UUID
	WorkerID uuid.UUID
	Err      error
}

func (d DiscardedEventParams) valid() error {
	if d.Err == nil {
		return NilEventErr
	}
	return nil
}

func (r *RealtimeMailDB) MarkOutboxEventAsDiscarded(ctx context.Context, p DiscardedEventParams) (bool, error) {
	err := p.valid()
	if err != nil {
		return false, err
	}
	rows, err := r.queries.MarkOutboxEventAsDiscarded(ctx, realtimemailsql.MarkOutboxEventAsDiscardedParams{
		OutboxEventID: pgtype.UUID{
			Bytes: p.EventID,
			Valid: true,
		},
		LastError: pgtype.Text{
			String: p.Err.Error(),
			Valid:  true,
		},
		LockedBy: pgtype.Text{
			String: p.WorkerID.String(),
			Valid:  true,
		},
	})
	if err != nil {
		return false, &DbQueryError{
			QueryName: "MarkOutboxEventAsDiscarded",
			Err:       err,
		}
	}

	return rows == int64(1), nil
}
