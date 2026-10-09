package clients

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Client interface {
	SendMessage(context.Context, MessageRequest) error
	Close() error
}

type MessageRequest struct {
	Topic   string
	Key     []byte
	Message []byte
	Headers []kafka.Header
}
