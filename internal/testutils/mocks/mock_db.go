package mocks

import (
	"context"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	ingestevents "github.com/alexis-dragneel/realtime-mail-agent/internal/server/models/ingest_events"
	"github.com/google/uuid"
)

type MockDB struct {
	CreateEventsErr               error
	ClaimOutboxEventsRes          []realtimemailsql.OutboxEvent
	ClaimOutboxEventsErr          error
	MarkOutboxEventAsPublishedRes bool
	MarkOutboxEventAsPublishedErr error
	MarkOutboxEventAsFailedRes    bool
	MarkOutboxEventAsFailedErr    error
	MarkOutboxEventAsDiscardedRes bool
	MarkOutboxEventAsDiscardedErr error
}

func (m *MockDB) CreateEvents(context.Context, *ingestevents.IngestEvent) error {
	return m.CreateEventsErr
}

func (m *MockDB) ClaimOutboxEvents(context.Context, db.ClaimOutboxEventsParams) ([]realtimemailsql.OutboxEvent, error) {
	return m.ClaimOutboxEventsRes, m.ClaimOutboxEventsErr
}

func (m *MockDB) MarkOutboxEventAsPublished(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
	return m.MarkOutboxEventAsPublishedRes, m.MarkOutboxEventAsPublishedErr
}

func (m *MockDB) MarkOutboxEventAsFailed(context.Context, db.FailedEventParams) (bool, error) {
	return m.MarkOutboxEventAsFailedRes, m.MarkOutboxEventAsFailedErr
}

func (m *MockDB) MarkOutboxEventAsDiscarded(context.Context, db.DiscardedEventParams) (bool, error) {
	return m.MarkOutboxEventAsDiscardedRes, m.MarkOutboxEventAsDiscardedErr
}
