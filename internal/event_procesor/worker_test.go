package eventprocesor

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/event_procesor/processors"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

type mockDB struct {
	claimFunc     func() ([]realtimemailsql.OutboxEvent, error)
	markPublished func(uuid.UUID, uuid.UUID, map[string]uuid.UUIDs) (bool, error)
	markFailed    func(uuid.UUID, uuid.UUID, time.Time, map[string]uuid.UUIDs) (bool, error)
	markDiscarded func(uuid.UUID, uuid.UUID, map[string]uuid.UUIDs) (bool, error)

	eventStorage map[string]uuid.UUIDs
}

func (m *mockDB) ClaimOutboxEvents(_ context.Context, _ db.ClaimOutboxEventsParams) ([]realtimemailsql.OutboxEvent, error) {
	return m.claimFunc()
}

func (m *mockDB) MarkOutboxEventAsPublished(_ context.Context, eventID, workerID uuid.UUID) (bool, error) {
	return m.markPublished(eventID, workerID, m.eventStorage)
}

func (m *mockDB) MarkOutboxEventAsFailed(_ context.Context, p db.FailedEventParams) (bool, error) {
	return m.markFailed(p.EventID, p.WorkerID, p.NextAttemptAt, m.eventStorage)
}

func (m *mockDB) MarkOutboxEventAsDiscarded(_ context.Context, p db.DiscardedEventParams) (bool, error) {
	return m.markDiscarded(p.EventID, p.WorkerID, m.eventStorage)
}

type mockProcessor struct {
	processed int
	err       error
}

func (p *mockProcessor) Process(context.Context, processors.Job) error {
	p.processed += 1
	return p.err
}

func Test_worker_work(t *testing.T) {
	tcs := []struct {
		name string
		test func(*testing.T)
	}{
		{
			name: "it pass to success when process does not return an error",
			test: func(*testing.T) {
				ctx, done := context.WithCancel(t.Context())
				eventID := uuid.New()
				db := &mockDB{
					claimFunc: func() (_ []realtimemailsql.OutboxEvent, _ error) {
						return []realtimemailsql.OutboxEvent{
							{
								ID: pgtype.UUID{
									Bytes: eventID,
									Valid: true,
								},
							},
						}, nil
					},
					markPublished: func(eventID uuid.UUID, _ uuid.UUID, storage map[string]uuid.UUIDs) (bool, error) {
						defer done()
						_, ok := storage["published"]
						if !ok {
							storage["published"] = uuid.UUIDs{}
						}
						storage["published"] = append(storage["published"], eventID)
						return true, nil
					},
					markFailed: func(_ uuid.UUID, _ uuid.UUID, _ time.Time, _ map[string]uuid.UUIDs) (bool, error) {
						return false, nil
					},
					markDiscarded: func(_ uuid.UUID, _ uuid.UUID, _ map[string]uuid.UUIDs) (bool, error) {
						return false, nil
					},
					eventStorage: map[string]uuid.UUIDs{},
				}

				mockP := &mockProcessor{processed: 0}

				worker, err := newEventWorker(workerEventSettings{
					db:        db,
					processor: mockP,

					eventLimit:       10,
					leaseDurationSec: 30 * 60,

					intervalSec: 10,
					jitter:      2,
					backoffSec:  5 * 60,
				})
				if err != nil {
					t.Fatalf("invalid worker settings: %s", err.Error())
				}
				go worker.work(ctx)
				<-ctx.Done()
				if mockP.processed < 1 {
					t.Fatalf("expecting the event %s, to be processed", eventID.String())
				}

				if !slices.Contains(db.eventStorage["published"], eventID) {
					t.Fatalf("expecting the event %s, to be included in the published storage", eventID)
				}
			},
		},
		{
			name: "it pass to failed when process does return an error retriable",
			test: func(*testing.T) {
				ctx, done := context.WithCancel(t.Context())
				eventID := uuid.New()
				db := &mockDB{
					claimFunc: func() (_ []realtimemailsql.OutboxEvent, _ error) {
						return []realtimemailsql.OutboxEvent{
							{
								ID: pgtype.UUID{
									Bytes: eventID,
									Valid: true,
								},
								Attempts:    1,
								MaxAttempts: 5,
							},
						}, nil
					},
					markPublished: func(_ uuid.UUID, _ uuid.UUID, _ map[string]uuid.UUIDs) (bool, error) {
						return false, nil
					},
					markFailed: func(eventID uuid.UUID, _ uuid.UUID, _ time.Time, storage map[string]uuid.UUIDs) (bool, error) {
						defer done()
						_, ok := storage["failed"]
						if !ok {
							storage["failed"] = uuid.UUIDs{}
						}
						storage["failed"] = append(storage["failed"], eventID)
						return true, nil
					},
					markDiscarded: func(_ uuid.UUID, _ uuid.UUID, _ map[string]uuid.UUIDs) (bool, error) {
						return false, nil
					},
					eventStorage: map[string]uuid.UUIDs{},
				}

				mockP := &mockProcessor{processed: 0, err: processors.ProcessError{
					Err:   errors.New("i can retry"),
					Retry: true,
				}}

				worker, err := newEventWorker(workerEventSettings{
					db:        db,
					processor: mockP,

					eventLimit:       10,
					leaseDurationSec: 30 * 60,

					intervalSec: 10,
					jitter:      2,
					backoffSec:  5 * 60,
				})
				if err != nil {
					t.Fatalf("invalid worker settings: %s", err.Error())
				}
				go worker.work(ctx)
				<-ctx.Done()
				if mockP.processed < 1 {
					t.Fatalf("expecting the event %s, to be processed", eventID.String())
				}
				if !slices.Contains(db.eventStorage["failed"], eventID) {
					t.Fatalf("expecting the event %s, to be included in the failed storage", eventID)
				}
			},
		},
		{
			name: "it pass to discarded when error is not retriable",
			test: func(*testing.T) {
				ctx, done := context.WithCancel(t.Context())
				eventID := uuid.New()
				db := &mockDB{
					claimFunc: func() (_ []realtimemailsql.OutboxEvent, _ error) {
						return []realtimemailsql.OutboxEvent{
							{
								ID: pgtype.UUID{
									Bytes: eventID,
									Valid: true,
								},
							},
						}, nil
					},
					markPublished: func(_ uuid.UUID, _ uuid.UUID, _ map[string]uuid.UUIDs) (bool, error) {
						return false, nil
					},
					markFailed: func(eventID uuid.UUID, _ uuid.UUID, _ time.Time, storage map[string]uuid.UUIDs) (bool, error) {
						return false, nil
					},
					markDiscarded: func(eventID uuid.UUID, _ uuid.UUID, storage map[string]uuid.UUIDs) (bool, error) {
						defer done()
						_, ok := storage["discarded"]
						if !ok {
							storage["discarded"] = uuid.UUIDs{}
						}
						storage["discarded"] = append(storage["discarded"], eventID)
						return true, nil
					},
					eventStorage: map[string]uuid.UUIDs{},
				}

				mockP := &mockProcessor{processed: 0, err: processors.ProcessError{
					Err:   errors.New("i can not retry"),
					Retry: false,
				}}

				worker, err := newEventWorker(workerEventSettings{
					db:        db,
					processor: mockP,

					eventLimit:       10,
					leaseDurationSec: 30 * 60,

					intervalSec: 10,
					jitter:      2,
					backoffSec:  5 * 60,
				})
				if err != nil {
					t.Fatalf("invalid worker settings: %s", err.Error())
				}
				go worker.work(ctx)
				<-ctx.Done()
				if mockP.processed < 1 {
					t.Fatalf("expecting the event %s, to be processed", eventID.String())
				}
				if !slices.Contains(db.eventStorage["discarded"], eventID) {
					t.Fatalf("expecting the event %s, to be included in the discarded storage", eventID)
				}
			},
		},
		{
			name: "it passes to discarded when event is retriable but max attempts reached",
			test: func(*testing.T) {
				ctx, done := context.WithCancel(t.Context())
				eventID := uuid.New()
				db := &mockDB{
					claimFunc: func() (_ []realtimemailsql.OutboxEvent, _ error) {
						return []realtimemailsql.OutboxEvent{
							{
								ID: pgtype.UUID{
									Bytes: eventID,
									Valid: true,
								},
								Attempts:    5,
								MaxAttempts: 5,
							},
						}, nil
					},
					markPublished: func(_ uuid.UUID, _ uuid.UUID, _ map[string]uuid.UUIDs) (bool, error) {
						return false, nil
					},
					markFailed: func(eventID uuid.UUID, _ uuid.UUID, _ time.Time, storage map[string]uuid.UUIDs) (bool, error) {
						return false, nil
					},
					markDiscarded: func(eventID uuid.UUID, _ uuid.UUID, storage map[string]uuid.UUIDs) (bool, error) {
						defer done()
						_, ok := storage["discarded"]
						if !ok {
							storage["discarded"] = uuid.UUIDs{}
						}
						storage["discarded"] = append(storage["discarded"], eventID)
						return true, nil
					},
					eventStorage: map[string]uuid.UUIDs{},
				}

				mockP := &mockProcessor{processed: 0, err: processors.ProcessError{
					Err:   errors.New("i can retry"),
					Retry: true,
				}}

				worker, err := newEventWorker(workerEventSettings{
					db:        db,
					processor: mockP,

					eventLimit:       10,
					leaseDurationSec: 30 * 60,

					intervalSec: 10,
					jitter:      2,
					backoffSec:  5 * 60,
				})
				if err != nil {
					t.Fatalf("invalid worker settings: %s", err.Error())
				}
				go worker.work(ctx)
				<-ctx.Done()
				if mockP.processed < 1 {
					t.Fatalf("expecting the event %s, to be processed", eventID.String())
				}
				if !slices.Contains(db.eventStorage["discarded"], eventID) {
					t.Fatalf("expecting the event %s, to be included in the discarded storage", eventID)
				}
			},
		},
		{
			name: "it discard an event when max attempt was passed without process the event",
			test: func(*testing.T) {
				ctx, done := context.WithCancel(t.Context())
				eventID := uuid.New()
				db := &mockDB{
					claimFunc: func() (_ []realtimemailsql.OutboxEvent, _ error) {
						return []realtimemailsql.OutboxEvent{
							{
								ID: pgtype.UUID{
									Bytes: eventID,
									Valid: true,
								},
								Attempts:    6,
								MaxAttempts: 5,
							},
						}, nil
					},
					markPublished: func(_ uuid.UUID, _ uuid.UUID, _ map[string]uuid.UUIDs) (bool, error) {
						return false, nil
					},
					markFailed: func(eventID uuid.UUID, _ uuid.UUID, _ time.Time, storage map[string]uuid.UUIDs) (bool, error) {
						return false, nil
					},
					markDiscarded: func(eventID uuid.UUID, _ uuid.UUID, storage map[string]uuid.UUIDs) (bool, error) {
						defer done()
						_, ok := storage["discarded"]
						if !ok {
							storage["discarded"] = uuid.UUIDs{}
						}
						storage["discarded"] = append(storage["discarded"], eventID)
						return true, nil
					},
					eventStorage: map[string]uuid.UUIDs{},
				}

				mockP := &mockProcessor{processed: 0, err: nil}

				worker, err := newEventWorker(workerEventSettings{
					db:        db,
					processor: mockP,

					eventLimit:       10,
					leaseDurationSec: 30 * 60,

					intervalSec: 10,
					jitter:      2,
					backoffSec:  5 * 60,
				})
				if err != nil {
					t.Fatalf("invalid worker settings: %s", err.Error())
				}
				go worker.work(ctx)
				<-ctx.Done()
				if mockP.processed > 0 {
					t.Fatalf("expecting the event %s, to not be processed", eventID.String())
				}
				if !slices.Contains(db.eventStorage["discarded"], eventID) {
					t.Fatalf("expecting the event %s, to be included in the discarded storage", eventID)
				}
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, tc.test)
	}
}
