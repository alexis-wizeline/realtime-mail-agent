package processors

import (
	"context"
	"errors"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/clients"
	"github.com/segmentio/kafka-go"
)

type KafkaProcessor struct {
	c *clients.KafkaClient
}

func NewKafkaProcessor(client *clients.KafkaClient) *KafkaProcessor {
	return &KafkaProcessor{
		c: client,
	}
}

func (k *KafkaProcessor) Process(ctx context.Context, j Job) error {
	err := k.c.SendMessage(ctx, j.Topic, j.EventType, j.Payload)
	if err != nil {
		var kafkaErr kafka.Error
		ok := errors.As(err, &kafkaErr)
		if !ok {
			return err
		}

		return ProcessError{
			Err:   kafkaErr,
			Retry: kafkaErr.Temporary(),
		}
	}

	return nil
}
