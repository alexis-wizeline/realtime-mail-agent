package db

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
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
