package processors

import (
	"context"

	"github.com/google/uuid"
)

type ProcessError struct {
	err error
}

func (p ProcessError) Error() string {
	return p.err.Error()
}

// TODO: Handle this correctly
func (p ProcessError) Retry() bool {
	return true
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
