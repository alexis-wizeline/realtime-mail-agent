package testingutils

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	ingestevents "github.com/alexis-dragneel/realtime-mail-agent/internal/server/models/ingest_events"
)

type EventIDs []string
type TestutilsOutboxEvent struct {
	ID                 string
	Status             string
	AssignedWorkerName pgtype.Text
}

type doneFunc func()
type TestutilsDB struct {
	DB db.DB

	pool *pgxpool.Pool
	done doneFunc
}

func NewTestutilsDB(ctx context.Context, t *testing.T) *TestutilsDB {
	pool, done := setupTestDB(ctx, t)

	return &TestutilsDB{
		DB: db.NewRealtimeMailDB(pool, db.DefaultOutboxMapper),

		pool: pool,
		done: done,
	}
}

func (t *TestutilsDB) Done() {
	t.done()
}

func (t *TestutilsDB) AddOutboxEvents(ctx context.Context, quantity int) (EventIDs, error) {
	eventIDs := make(EventIDs, quantity)
	for i := range quantity {
		eventID := uuid.NewString()
		event := &ingestevents.IngestEvent{
			EventID:   eventID,
			UserID:    uuid.NewString(),
			MessageID: uuid.NewString(),
			Type:      "testutils.Payload",
		}
		err := t.DB.CreateEvents(ctx, event)
		if err != nil {
			return nil, err
		}
		eventIDs[i] = eventID
	}

	return eventIDs, nil
}

func (t *TestutilsDB) CleanupDB(ctx context.Context) error {
	rows, err := t.pool.Query(ctx, "DELETE FROM outbox_events;")
	if err != nil {
		return err
	}
	defer func() {
		if rows == nil {
			return
		}
		rows.Close()
	}()
	incoming, err := t.pool.Query(ctx, "DELETE FROM incoming_events;")
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

func (t *TestutilsDB) QueryEventStatuses(ctx context.Context, ids EventIDs) ([]TestutilsOutboxEvent, error) {
	IDsQuery := `SELECT id FROM incoming_events WHERE event_id = ANY($1)`
	incommingRows, err := t.pool.Query(ctx, IDsQuery, ids)
	if err != nil {
		return nil, err
	}
	defer incommingRows.Close()

	var incommingEventsIDS []string
	for incommingRows.Next() {
		var id string
		err := incommingRows.Scan(&id)
		if err != nil {
			return nil, err
		}

		incommingEventsIDS = append(incommingEventsIDS, id)
	}

	outboxQuery := `SELECT id, status, locked_by FROM outbox_events WHERE incoming_event_id = ANY($1)`
	outboxRows, err := t.pool.Query(ctx, outboxQuery, incommingEventsIDS)
	if err != nil {
		return nil, err
	}
	defer outboxRows.Close()

	var events []TestutilsOutboxEvent
	for outboxRows.Next() {
		var event TestutilsOutboxEvent
		err := outboxRows.Scan(&event.ID, &event.Status, &event.AssignedWorkerName)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	return events, nil
}

func setupTestDB(ctx context.Context, t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("unable to initialize the db: %s", err.Error())
	}

	err = pool.Ping(ctx)
	if err != nil {
		t.Fatalf("unable to connect with the db: %s", err.Error())
	}

	return pool, func() {
		pool.Close()
	}
}
