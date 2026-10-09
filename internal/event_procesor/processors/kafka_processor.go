package processors

import (
	"context"
	"errors"
	"io"
	"syscall"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/clients"
	"github.com/segmentio/kafka-go"
)

type KafkaProcessor struct {
	c clients.Client
}

func NewKafkaProcessor(client clients.Client) Processor {
	return &KafkaProcessor{
		c: client,
	}
}

func (k *KafkaProcessor) Process(ctx context.Context, j Job) error {
	err := k.c.SendMessage(ctx, clients.MessageRequest{
		Topic:   j.Topic,
		Key:     []byte(j.Key),
		Message: j.Payload,
		Headers: []kafka.Header{
			{
				Key:   "Content-Type",
				Value: []byte("application/json"),
			},
			{
				Key:   "job-id",
				Value: j.ID[:],
			},
		},
	})
	if err != nil {
		return toProcessError(err)
	}

	return nil
}

func toProcessError(err error) ProcessError {
	retry := errors.Is(err, kafka.UnknownTopicOrPartition) ||
		errors.Is(err, kafka.NetworkException) ||
		errors.Is(err, kafka.NotEnoughReplicas) ||
		errors.Is(err, kafka.NotEnoughReplicasAfterAppend) ||
		errors.Is(err, kafka.BrokerNotAvailable) ||
		errors.Is(err, kafka.RequestTimedOut) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET)

	return ProcessError{
		Err:   err,
		Retry: retry,
	}
}
