package clients

import (
	"context"
	"errors"
	"strings"

	"github.com/segmentio/kafka-go"
)

var (
	EmptyTopicErr   = errors.New("Topic is empty")
	EmptyMessageErr = errors.New("Message is empty")
)

type KafkaClient struct {
	w *kafka.Writer
}

func NewKafkaCLient(url string) *KafkaClient {
	return &KafkaClient{
		w: &kafka.Writer{
			Addr:                   kafka.TCP(url),
			AllowAutoTopicCreation: true,
		},
	}
}

type MessageRequest struct {
	Topic   string
	Key     []byte
	Message []byte
	Headers []kafka.Header
}

func (m MessageRequest) valid() error {
	if strings.Trim(m.Topic, " ") == "" {
		return EmptyTopicErr
	}

	if len(m.Message) == 0 {
		return EmptyMessageErr
	}

	return nil
}

func (k *KafkaClient) SendMessage(ctx context.Context, m MessageRequest) error {
	if err := m.valid(); err != nil {
		return err
	}

	return k.w.WriteMessages(ctx, kafka.Message{
		Topic:   m.Topic,
		Key:     m.Key,
		Value:   m.Message,
		Headers: m.Headers,
	})
}

func (k *KafkaClient) Close() error {
	return k.w.Close()
}
