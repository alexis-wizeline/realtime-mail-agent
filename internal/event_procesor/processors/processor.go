package processors

import (
	"context"

	"github.com/google/uuid"
)

type ProcessError struct {
	Err   error
	Retry bool
}

func (p ProcessError) Error() string {
	return p.Err.Error()
}

// TODO: Handle this correctly
func (p ProcessError) Retriable() bool {
	return p.Retry
}

type Processor interface {
	Process(context.Context, Job) error
}

type Job struct {
	ID        uuid.UUID
	EventType string
	Topic     string
	Payload   []byte
}
