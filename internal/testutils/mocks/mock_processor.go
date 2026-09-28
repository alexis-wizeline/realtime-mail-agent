package mocks

import (
	"context"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/event_procesor/processors"
)

type MockProcessFn func(context.Context, processors.Job) error

type MockProcessor struct {
	exec MockProcessFn
}

func NewMockProcess(p MockProcessFn) processors.Processor {
	return MockProcessor{
		exec: p,
	}
}

func (m MockProcessor) Process(ctx context.Context, j processors.Job) error {
	if m.exec != nil {
		return m.exec(ctx, j)
	}
	return nil
}
