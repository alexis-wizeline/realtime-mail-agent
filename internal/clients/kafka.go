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

func (k *KafkaClient) SendMessage(ctx context.Context, topic, key string, message []byte) error {
	if strings.Trim(topic, " ") == "" {
		return EmptyTopicErr
	}
	if len(message) == 0 {
		return EmptyMessageErr
	}

	return k.w.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: message,
	})
}

func (k *KafkaClient) Close() error {
	return k.w.Close()
}
