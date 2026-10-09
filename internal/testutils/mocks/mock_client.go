package mocks

import (
	"context"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/clients"
)

type MockClient struct {
	Err error
}

func (m *MockClient) SendMessage(context.Context, clients.MessageRequest) error {
	return m.Err
}

func (m *MockClient) Close() error {
	return nil
}
