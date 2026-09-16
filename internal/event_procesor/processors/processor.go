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
	if unwrap := p.Unwrap(); unwrap != nil {
		return unwrap.Error()
	}
	return ""
}

func (p ProcessError) Unwrap() error {
	return p.Err
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
