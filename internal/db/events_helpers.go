package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	ingestevents "github.com/alexis-dragneel/realtime-mail-agent/internal/server/models/ingest_events"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type OutboxEvent struct {
	incomingEventID uuid.UUID
	eventType       string
	topic           string
	schemaVersion   string
}

func (o *OutboxEvent) serialize() ([]byte, error) {
	return json.Marshal(o)
}

type OutboxMapperFunc func(incomingEventID uuid.UUID, e *ingestevents.IngestEvent) OutboxEvent

var DefaultOutboxMapper OutboxMapperFunc = func(incomingEventID uuid.UUID, e *ingestevents.IngestEvent) OutboxEvent {
	return OutboxEvent{
		incomingEventID: incomingEventID,
		eventType:       e.Type,
		topic:           "events",
		schemaVersion:   "1",
	}
}

func createIncomingEvent(ctx context.Context, qTx *realtimemailsql.Queries, e *ingestevents.IngestEvent) (uuid.UUID, bool, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.UUID{}, false, err
	}
	buf, err := e.Serialize()
	if err != nil {
		return uuid.UUID{}, false, fmt.Errorf("unable to serialize the payload: %s", err)
	}
	_, err = qTx.CreateIncomingEvent(ctx,
		realtimemailsql.CreateIncomingEventParams{
			ID: pgtype.UUID{
				Bytes: id,
				Valid: true,
			},
			EventID:   e.EventID,
			UserID:    e.UserID,
			MessageID: e.MessageID,
			EventType: e.Type,
			Payload:   buf,
		})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// TODO validate if the payload is malformed
			return uuid.UUID{}, false, nil
		}
		return uuid.UUID{}, false, err
	}

	return id, true, nil
}

type createOutboxEventParams struct {
	incommingEventID uuid.UUID
	payload          *ingestevents.IngestEvent
}

func createOutbocEvent(ctx context.Context, qTx *realtimemailsql.Queries, e OutboxEvent) error {
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	buf, err := e.serialize()
	if err != nil {
		return fmt.Errorf("unable to serialize the payload: %s", err)
	}
	_, err = qTx.CreateOutboxEvent(ctx,
		realtimemailsql.CreateOutboxEventParams{
			ID: pgtype.UUID{
				Bytes: id,
				Valid: true,
			},
			IncomingEventID: pgtype.UUID{
				Bytes: e.incomingEventID,
				Valid: true,
			},
			EventType: e.eventType,
			Topic:     e.topic,
			Payload:   buf,
		})
	if err != nil {
		return err
	}

	return nil
}
