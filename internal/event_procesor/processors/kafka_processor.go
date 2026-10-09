package processors

import (
	"context"
	"io"
	"syscall"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/clients"
	"github.com/segmentio/kafka-go"
)

type KafkaProcessor struct {
	c clients.Client
}

func NewKafkaProcessor(client *clients.KafkaClient) *KafkaProcessor {
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
	retry := false
	switch err {
	case kafka.UnknownTopicOrPartition,
		kafka.NetworkException,
		kafka.InvalidRequest,
		kafka.NotEnoughReplicas,
		kafka.NotEnoughReplicasAfterAppend,
		kafka.BrokerNotAvailable,
		kafka.RequestTimedOut,
		context.DeadlineExceeded,
		context.Canceled,
		io.ErrUnexpectedEOF,
		syscall.ECONNREFUSED,
		syscall.ECONNRESET:
		retry = true
	default:
	}

	return ProcessError{
		Err:   err,
		Retry: retry,
	}
}
