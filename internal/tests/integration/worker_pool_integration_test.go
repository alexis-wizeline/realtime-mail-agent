package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/clients"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	eventprocesor "github.com/alexis-dragneel/realtime-mail-agent/internal/event_procesor"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/event_procesor/processors"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/logger"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/testutils"
	"github.com/google/uuid"
)

func Test_worker_pool_integration_test(t *testing.T) {
	consumer := testutils.NewTestConsumer(t, os.Getenv("KAFKA_URL"), db.GLOBAL_TOPIC)
	defer consumer.Close()

	dbUtils := testutils.NewTestutilsDB(t.Context(), t)
	ctx, done := context.WithTimeout(t.Context(), 20*time.Second)
	eventIDs, err := dbUtils.AddOutboxEvents(ctx, 20)
	if err != nil {
		t.Fatalf("AddOutboxEvents() err: %s", err)
	}

	errCH, doneCh := make(chan error, 1), make(chan struct{}, 1)
	go func() {
		tic := time.NewTicker(2 * time.Second)
		defer func() {
			close(errCH)
			close(doneCh)
			tic.Stop()
		}()
		for {
			select {
			case <-tic.C:
			case <-ctx.Done():
				return
			}

			events, err := dbUtils.QueryEventStatuses(ctx, eventIDs)
			if err != nil {
				errCH <- err
				return
			}

			total := 0
			for _, event := range events {
				if event.Status != "published" {
					continue
				}
				total++
			}

			if total == len(events) {
				return
			}
		}
	}()

	start := time.Now()
	pool, err := eventprocesor.NewEventProcessorPool(ctx, eventprocesor.NewEventProcessorPoolParams{
		WorkerSettings: eventprocesor.ProcessorWorkerPoolSettings{
			Workers:            4,
			EventsWorkerLimit:  5,
			EventLeaseDuration: 30 * time.Second,
			WorkerInterval:     5 * time.Second,
			WorkerBackoff:      2 * time.Second,
			WorkerJitter:       20 * time.Millisecond,
		},
		DB:        dbUtils.DB,
		Processor: processors.NewKafkaProcessor(clients.NewKafkaClient(os.Getenv("KAFKA_URL"))),
		Logger:    &logger.Logger{},
	})
	if err != nil {
		t.Fatalf("NewEventProcessorPool err: %s", err)
	}

	go pool.Start()
	<-doneCh
	end := time.Now()
	for err := range errCH {
		if err != nil {
			t.Fatal(err)
		}
	}

	messages, err := consumer.ReadMessagesFromAtToAt(ctx, start, end, len(eventIDs))
	if err != nil {
		t.Fatalf("consumer read messages err: %s", err)
	}

	if len(messages) != len(eventIDs) {
		t.Fatalf("messages does not match the number of events want: %d, got: %d", len(eventIDs), len(messages))
	}

	processedJobIDs := make(map[uuid.UUID]struct{})
	for _, m := range messages {
		for _, h := range m.Headers {
			if h.Key != "job-id" {
				continue
			}

			id, err := uuid.ParseBytes(h.Value)
			if err != nil {
				t.Fatalf("parsing job id form kafka message err: %s", err)
			}
			processedJobIDs[id] = struct{}{}
		}
	}

	outboxEvents, err := dbUtils.QueryEventStatuses(ctx, eventIDs)
	if err != nil {
		t.Fatalf("QueryEventStatuses err: %s", err)
	}

	for _, outboxEvent := range outboxEvents {
		id, err := uuid.Parse(outboxEvent.ID)
		if err != nil {
			t.Fatalf("UUID parse from outbox event: %s", err)
		}

		_, ok := processedJobIDs[id]
		if !ok {
			t.Fatalf("event %s, not published", outboxEvent.ID)
		}
	}

	err = dbUtils.CleanupDB(ctx)
	if err != nil {
		t.Fatalf("CleanupDB err: %s", err)
	}
	done()
}
