package db

import (
	"context"
	"errors"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	ingestevents "github.com/alexis-dragneel/realtime-mail-agent/internal/server/models/ingest_events"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var dupliateIncominEventErr = errors.New("The incomming event to strore is duplicated")

func createIncomingEvent(ctx context.Context, qTx *realtimemailsql.Queries, s Serializable[*ingestevents.IngestEvent]) (uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.UUID{}, err
	}
	_, err = qTx.CreateIncomingEvent(ctx,
		realtimemailsql.CreateIncomingEventParams{
			ID: pgtype.UUID{
				Bytes: id,
				Valid: true,
			},
			EventID:   s.data.EventID,
			UserID:    s.data.UserID,
			MessageID: s.data.MessageID,
			EventType: s.data.Type,
			Payload:   s.buf,
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
	payload          Serializable[*ingestevents.IngestEvent]
}

func createOutbocEvent(ctx context.Context, qTx *realtimemailsql.Queries, p createOutboxEventParams) error {
	id, err := uuid.NewV7()
	if err != nil {
		return err
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
			EventType: p.payload.data.Type,
			Topic:     "events",
			Payload:   p.payload.buf,
		})
	if err != nil {
		return err
	}

	return nil
}
