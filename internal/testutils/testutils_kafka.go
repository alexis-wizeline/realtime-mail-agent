package testutils

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type TestUtilsKafkaConsumer struct {
	r *kafka.Reader
}

func NewTestConsumer(t *testing.T, url, topic string) *TestUtilsKafkaConsumer {
	t.Helper()

	k := &TestUtilsKafkaConsumer{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{url},
			Topic:   topic,
		}),
	}

	conn, err := kafka.Dial("tcp", url)
	if err != nil {
		t.Fatalf("unable to connect to kafka: %s", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil && !errors.Is(err, kafka.UnknownTopicOrPartition) {
		t.Fatalf("Unable to comunicate with fakfa: %s", err)
	}

	if len(partitions) != 0 {
		return k
	}

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("unable to get kafka controller: %s", err)
	}

	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("unable to connect to kafka controller: %s", err)
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})

	if err != nil {
		t.Fatalf("unable to create topic %s: %s", topic, err)
	}

	return k
}

func (t *TestUtilsKafkaConsumer) ReadMessagesFromAtToAt(ctx context.Context, startsAt, endsAt time.Time, max int) ([]kafka.Message, error) {
	err := t.r.SetOffsetAt(ctx, startsAt)
	if err != nil {
		return nil, err
	}
	var messages []kafka.Message
	for {
		m, err := t.r.ReadMessage(ctx)
		if err != nil {
			return messages, err
		}
		if m.Time.After(endsAt) {
			return messages, nil
		}

		messages = append(messages, m)
		if len(messages) == max {
			return messages, nil
		}
	}
}

func (t *TestUtilsKafkaConsumer) Close() error {
	return t.r.Close()
}
