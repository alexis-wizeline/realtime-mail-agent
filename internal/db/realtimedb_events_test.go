package db

import (
	"context"
	"fmt"
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
	db := &RealtimeMailDB{pool: pool, queries: queries}
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

				rows, err := pool.Query(ctx,
					`SELECT id FROM
					incoming_events
					WHERE event_id = $1;`,
					ingestEvent.EventID)
				if err != nil {
					t.Fatalf("Not able to retrive the created event: %s", err)
				}
				defer rows.Close()

				fmt.Println(rows.Err(), ingestEvent.EventID)
				if !rows.Next() {
					t.Fatal("incomming event not created")
				}

				if rows.Next() {
					t.Fatal("Expecting only a single event being created")
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

				rows, err := pool.Query(ctx,
					`SELECT id FROM
				incoming_events
				WHERE event_id = $1;`,
					ingestEvent.EventID)
				if err != nil {
					t.Fatalf("Not able to retrive the created event: %s", err)
				}
				defer rows.Close()

				fmt.Println(rows.Err(), ingestEvent.EventID)
				if !rows.Next() {
					t.Fatal("incomming event not created")
				}

				if rows.Next() {
					t.Fatal("Expecting only a single event being created")
				}
			},
		},
		{
			name: "concurrent calls create a single event",
			test: func(t *testing.T) {
				readyCh := make(chan struct{})
				wg := sync.WaitGroup{}
				ingestEvent := newEventPayload()
				for range 2 {
					wg.Go(func() {
						<-readyCh
						err := db.CreateEvents(ctx, ingestEvent)
						if err != nil {
							t.Fatalf("event not created, %s", err)
						}
					})
				}

				close(readyCh)
				wg.Wait()

				rows, err := pool.Query(ctx,
					`SELECT id FROM
				incoming_events
				WHERE event_id = $1;`,
					ingestEvent.EventID)
				if err != nil {
					t.Fatalf("Not able to retrive the created event: %s", err)
				}
				defer rows.Close()

				fmt.Println(rows.Err(), ingestEvent.EventID)
				if !rows.Next() {
					t.Fatal("incomming event not created")
				}

				if rows.Next() {
					t.Fatal("Expecting only a single event being created")
				}
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, tc.test)
	}

}

func newEventPayload() *ingestevents.IngestEvent {
	return &ingestevents.IngestEvent{
		EventID:   uuid.New().String(),
		UserID:    uuid.New().String(),
		MessageID: uuid.New().String(),
		Type:      "test.payload",
	}
}
