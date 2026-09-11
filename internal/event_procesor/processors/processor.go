package processors

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

var (
	// TODO: make correct errors and retry mechanism
	RetriableError    = errors.New("retry")
	NonRetriableError = errors.New("no retry")
)

type Processor interface {
	Process(context.Context, Job) error
}

type Job struct {
	ID        uuid.UUID
	EventType string
	Topic     string
	Payload   []byte
}
