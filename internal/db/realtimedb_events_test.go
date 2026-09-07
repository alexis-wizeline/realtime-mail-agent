package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	ingestevents "github.com/alexis-dragneel/realtime-mail-agent/internal/server/models/ingest_events"
)

func setupTestDB() (*pgxpool.Pool, *realtimemailsql.Queries, func(), error) {
	dbURL := os.Getenv("DATABASE_URL")
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, nil, nil, err
	}

	q := realtimemailsql.New(pool)
	return pool, q, func() {
		pool.Close()
	}, pool.Ping(ctx)
}

func Test_CreateEvents(t *testing.T) {
	pool, queries, finish, err := setupTestDB()
	if err != nil {
		t.Fatalf("unable to connect to db: %s", err)
	}
	defer finish()
	db := &RealtimeMailDB{pool: pool, queries: queries, outboxEventMapper: DefaultOutboxMapper}
	ctx := context.Background()
	tcs := []struct {
		name string
		test func(*testing.T)
	}{
		{
			name: "single call save incomming and outbox events",
			test: func(t *testing.T) {
				ingestEvent := newEventPayload()
				err := db.CreateEvents(ctx, ingestEvent)
				if err != nil {
					t.Fatalf("event not created, %s", err)
				}

				insertedRowsCount := insertedEventsResult(ctx, pool, ingestEvent.EventID)
				if insertedRowsCount == nil {
					t.Fatal("insertedRowsCount scan failed")
				}
				if *insertedRowsCount != 1 {
					t.Fatal("the number of inserted incoming and outbox evets is distinct than 1")
				}
			},
		},
		{
			name: "subsequent calls create a single event",
			test: func(t *testing.T) {
				ingestEvent := newEventPayload()
				err := db.CreateEvents(ctx, ingestEvent)
				if err != nil {
					t.Fatalf("event not created, %s", err)
				}
				err = db.CreateEvents(ctx, ingestEvent)
				if err != nil {
					t.Fatalf("Subsequent call failed: %s", err)
				}

				insertedRowsCount := insertedEventsResult(ctx, pool, ingestEvent.EventID)
				if insertedRowsCount == nil {
					t.Fatal("insertedRowsCount scan failed")
				}
				if *insertedRowsCount != 1 {
					t.Fatal("the number of inserted incoming and outbox evets is distinct than 1")
				}
			},
		},
		{
			name: "concurrent calls create a single event",
			test: func(t *testing.T) {
				readyCh := make(chan struct{})
				errCh := make(chan error, 2)
				wg := sync.WaitGroup{}
				ingestEvent := newEventPayload()
				for range 2 {
					wg.Go(func() {
						<-readyCh
						errCh <- db.CreateEvents(ctx, ingestEvent)
					})
				}

				close(readyCh)
				wg.Wait()
				close(errCh)

				for err := range errCh {
					if err != nil {
						t.Fatalf("error while creating event: %s", err)
					}
				}

				insertedRowsCount := insertedEventsResult(ctx, pool, ingestEvent.EventID)
				if insertedRowsCount == nil {
					t.Fatal("insertedRowsCount scan failed")
				}
				if *insertedRowsCount != 1 {
					t.Fatal("the number of inserted incoming and outbox evets is distinct than 1")
				}
			},
		},
		{
			name: "rollsback when outbox event creation fails",
			test: func(t *testing.T) {
				currentMapper := db.outboxEventMapper
				defer func() {
					db.outboxEventMapper = currentMapper
				}()
				db.outboxEventMapper = func(_ uuid.UUID, e *ingestevents.IngestEvent) OutboxEvent {
					return OutboxEvent{
						IncomingEventID: uuid.UUID{},
						EventType:       "fail",
						Topic:           "will fail",
						SchemaVersion:   "bad schema",
					}
				}
				ingestEvent := newEventPayload()
				err = db.CreateEvents(ctx, ingestEvent)
				if err == nil {
					t.Fatalf("expecting the creation to fail")
				}

				incomingRow := pool.QueryRow(ctx, `
					SELECT count(*) FROM
					incoming_events
					WHERE event_id = $1
					`, ingestEvent.EventID)

				count := new(int)
				err = incomingRow.Scan(count)
				if err != nil {
					t.Fatalf("an error ocur while querying the incoming events count, %s", err)
				}
				if *count != 0 {
					t.Fatal("the incoming event should rollback when outbox event creation fails")
				}

				outboxRow := pool.QueryRow(ctx, `
					SELECT count(*) FROM
					outbox_events
					WHERE incoming_event_id =
					(SELECT id FROM incoming_events WHERE event_id = $1 )
					`, ingestEvent.EventID)
				err = outboxRow.Scan(count)
				if err != nil {
					t.Fatalf("an error ocur while queryin the outbox events count: %s", err)
				}
				if *count != 0 {
					t.Fatal("expectd for outbox creation to be rolledback")
				}

			},
		},
		{
			name: "subsequent calls with distinct payload",
			test: func(t *testing.T) {
				t.Skip("for future validatios")
				ingestEvent := newEventPayload()
				err := db.CreateEvents(ctx, ingestEvent)
				if err != nil {
					t.Fatalf("event not created, %s", err)
				}
				ingestEvent.Type = "bad type"
				err = db.CreateEvents(ctx, ingestEvent)
				if err != nil {
					t.Fatalf("Subsequent call failed: %s", err)
				}

				insertedRowsCount := insertedEventsResult(ctx, pool, ingestEvent.EventID)
				if insertedRowsCount == nil {
					t.Fatal("insertedRowsCount scan failed")
				}
				if *insertedRowsCount != 1 {
					t.Fatal("the number of inserted incoming and outbox evets is distinct than 1")
				}
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, tc.test)
	}

	err = cleanUpJobs(t.Context(), pool)
	if err != nil {
		t.Fatalf("cleanupJobs Failed: %s", err.Error())
	}

}

func Test_ClaimOutboxEvents(t *testing.T) {
	p, q, finish, err := setupTestDB()
	if err != nil {
		t.Fatalf("unable to connect to db: %s", err.Error())
	}
	defer finish()
	db := &RealtimeMailDB{queries: q, pool: p, outboxEventMapper: DefaultOutboxMapper}

	tcs := []struct {
		name string
		test func(*testing.T)
	}{
		{
			name: "concurrent jobs claims distinc jobs",
			test: func(t *testing.T) {
				var mu sync.Mutex
				var wg sync.WaitGroup

				jobsStore := make(map[string][]realtimemailsql.OutboxEvent)
				ready, errChan := make(chan struct{}), make(chan error, 2)

				err := setUpEvents(t.Context(), db, 2)
				if err != nil {
					t.Fatalf("Unable to set up events for this test: %s", err.Error())
				}
				for i := range 2 {
					wg.Go(func() {
						<-ready
						key := fmt.Sprintf("Worker_%v", i)
						jobs, err := db.ClaimOutboxEvents(t.Context(), ClaimOutboxEventsParams{
							WorkerName:  key,
							LockedUntil: time.Now().Add(2 * time.Minute),
							JobsLimit:   1,
						})
						if err != nil {
							errChan <- err
							return
						}

						mu.Lock()
						jobsStore[key] = jobs
						mu.Unlock()

						ids := outboxJobsIDs(jobs)
						totalRows, err := q.MarkOutboxEventsAsPublished(t.Context(), realtimemailsql.MarkOutboxEventsAsPublishedParams{
							LockedBy: pgtype.Text{
								String: key,
								Valid:  true,
							},
							OutboxEventIds: ids,
						})
						if err != nil {
							errChan <- err
							return
						}
						if totalRows != int64(len(ids)) {
							errChan <- fmt.Errorf("the marketed jobs as published are not matching want: %v, got: %v", len(ids), totalRows)
						}
					})
				}

				close(ready)
				wg.Wait()
				close(errChan)

				for err := range errChan {
					if err != nil {
						t.Fatalf("error whne claimint events: %s", err.Error())
					}
				}

				if len(jobsStore) != 2 {
					t.Fatalf("only one job claim events")
				}

				limitClamaibleJobs := 1
				claimedJobs := make(map[uuid.UUID]struct{})
				for worker, jobs := range jobsStore {
					if len(jobs) != limitClamaibleJobs {
						t.Fatalf("Worker: %s, Claimed: %v, Expected: %v", worker, len(jobs), limitClamaibleJobs)
					}

					for _, job := range jobs {
						_, ok := claimedJobs[job.ID.Bytes]
						if ok {
							t.Fatalf("Job ID %v already claimed by other worker", job.ID)
						}
						claimedJobs[job.ID.Bytes] = struct{}{}
					}
				}
			},
		},
		{
			name: "job is claimed after the lease expires",
			test: func(*testing.T) {
				err := setUpEvents(t.Context(), db, 5)
				if err != nil {
					t.Fatalf("unable to setup jobs: %s", err.Error())
				}

				workerJobs, err := db.ClaimOutboxEvents(t.Context(), ClaimOutboxEventsParams{
					WorkerName:  "worker_1",
					LockedUntil: time.Now(),
					JobsLimit:   5,
				})
				if err != nil {
					t.Fatalf("Unbale to claim jobs for first worker: %s", err.Error())
				}
				workerOneMap := make(map[uuid.UUID]struct{})
				for _, job := range workerJobs {
					workerOneMap[job.ID.Bytes] = struct{}{}
				}
				lastWorkername := "worker_2"
				workerJobs, err = db.ClaimOutboxEvents(t.Context(), ClaimOutboxEventsParams{
					WorkerName:  lastWorkername,
					LockedUntil: time.Now(),
					JobsLimit:   5,
				})
				if err != nil {
					t.Fatalf("Unbale to claim jobs for second worker: %s", err.Error())
				}
				ids := make([]pgtype.UUID, len(workerJobs))
				for i, job := range workerJobs {
					_, ok := workerOneMap[job.ID.Bytes]
					if !ok {
						t.Fatal("Expected second worker to claim same jobs than first call")
					}
					ids[i] = job.ID
				}

				totalRows, err := q.MarkOutboxEventsAsPublished(t.Context(), realtimemailsql.MarkOutboxEventsAsPublishedParams{
					OutboxEventIds: ids,
					LockedBy: pgtype.Text{
						String: lastWorkername,
						Valid:  true,
					},
				})
				if err != nil {
					t.Fatalf("Unable to clean jobs by mark as publish: %err", err)
				}
				if totalRows != int64(len(workerJobs)) {
					t.Fatalf("The amount of jobs markes as published does not match: expect %v, got: %v", len(workerJobs), totalRows)
				}
			},
		},
		{
			name: "handle empty params",
			test: func(*testing.T) {
				_, err := db.ClaimOutboxEvents(t.Context(), ClaimOutboxEventsParams{
					LockedUntil: time.Now(),
				})
				if !errors.Is(err, EmptyWorkerName) {
					t.Fatal("expecting error to be empty worker name error")
				}

				_, err = db.ClaimOutboxEvents(t.Context(), ClaimOutboxEventsParams{
					WorkerName:  "Valid",
					LockedUntil: time.Time{},
				})

				if !errors.Is(err, LockedTimeIsZero) {
					t.Fatal("expecting error to be lcoked time is zero error")
				}
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, tc.test)
	}

	err = cleanUpJobs(t.Context(), p)
	if err != nil {
		t.Fatalf("cleanupJobs Failed: %s", err)
	}
}

func insertedEventsResult(ctx context.Context, p *pgxpool.Pool, eventID string) *int {
	count := new(int)

	row := p.QueryRow(ctx,
		`SELECT count(*) FROM
	incoming_events
	JOIN outbox_events ON
	outbox_events.incoming_event_id = incoming_events.id
	WHERE incoming_events.event_id = $1;`,
		eventID)

	err := row.Scan(count)
	if err != nil {
		return nil
	}

	return count
}

func newEventPayload() *ingestevents.IngestEvent {
	return &ingestevents.IngestEvent{
		EventID:   uuid.New().String(),
		UserID:    uuid.New().String(),
		MessageID: uuid.New().String(),
		Type:      "test.payload",
	}
}

func setUpEvents(ctx context.Context, db DB, num int) error {
	for range num {
		event := newEventPayload()
		err := db.CreateEvents(ctx, event)
		if err != nil {
			return err
		}

	}

	return nil
}

func cleanUpJobs(ctx context.Context, p *pgxpool.Pool) error {
	rows, err := p.Query(ctx, "DELETE FROM outbox_events;")
	if err != nil {
		return err
	}
	defer func() {
		if rows == nil {
			return
		}
		rows.Close()
	}()
	incoming, err := p.Query(ctx, "DELETE FROM incoming_events;")
	defer func() {
		if incoming == nil {
			return
		}
		incoming.Close()
	}()
	if err != nil {
		return err
	}
	return nil
}
