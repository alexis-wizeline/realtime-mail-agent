package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	ingestevents "github.com/alexis-dragneel/realtime-mail-agent/internal/server/models/ingest_events"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var dupliateIncominEventErr = errors.New("The incomming event to strore is duplicated")

func createIncomingEvent(ctx context.Context, qTx *realtimemailsql.Queries, e *ingestevents.IngestEvent) (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.UUID{}, err
	}
	buf, err := e.Serialize()
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("unable to serialize the payload: %s", err)
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
			return uuid.UUID{}, dupliateIncominEventErr
		}
		return uuid.UUID{}, err
	}

	return id, nil
}

type createOutboxEventParams struct {
	incommingEventID uuid.UUID
	payload          *ingestevents.IngestEvent
}

func createOutbocEvent(ctx context.Context, qTx *realtimemailsql.Queries, p createOutboxEventParams) error {
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	buf, err := p.payload.Serialize()
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
				Bytes: p.incommingEventID,
				Valid: true,
			},
			EventType: p.payload.Type,
			Topic:     "events",
			Payload:   buf,
		})
	if err != nil {
		return err
	}

	return nil
}
