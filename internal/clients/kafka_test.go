package clients

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go"
)

type fackeWritter struct {
	err            error
	compareMsgFunc func(kafka.Message) error
}

func (f *fackeWritter) Close() error {
	return nil
}

func (f *fackeWritter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	if f.compareMsgFunc != nil {
		for _, msg := range msgs {
			err := f.compareMsgFunc(msg)
			if err != nil {
				return err
			}
		}
	}
	return f.err
}

func Test_kafkaClient_error(t *testing.T) {
	wildError := errors.New("a wild error has appear")
	invalidMsgError := errors.New("inavlid message formed")

	validMSg := kafka.Message{
		Topic: "events",
		Value: []byte("hi"),
		Key:   []byte("some key"),
		Headers: []kafka.Header{
			{
				Key:   "Content-Type",
				Value: []byte("application/json"),
			},
		},
	}
	validateMSG := func(msg kafka.Message) error {
		if !reflect.DeepEqual(validMSg, msg) {
			return invalidMsgError
		}
		return nil
	}

	tcs := []struct {
		name string

		writter messageWritter
		req     MessageRequest

		want error
	}{
		{
			name:    "succes",
			writter: &fackeWritter{},
			req:     MessageRequest{Topic: "events", Message: []byte("hi")},
			want:    nil,
		},
		{
			name:    "empty topic",
			writter: &fackeWritter{},
			req:     MessageRequest{Message: []byte("hi")},
			want:    EmptyTopicErr,
		},
		{
			name:    "empty message",
			writter: &fackeWritter{},
			req:     MessageRequest{Topic: "events"},
			want:    EmptyMessageErr,
		},
		{
			name:    "error is propagated",
			writter: &fackeWritter{err: wildError},
			req:     MessageRequest{Topic: "events", Message: []byte("hi")},
			want:    wildError,
		},
		{
			name:    "msg is correct mapped",
			writter: &fackeWritter{compareMsgFunc: validateMSG},
			req: MessageRequest{
				Topic:   "events",
				Message: []byte("hi"),
				Key:     []byte("some key"),
				Headers: []kafka.Header{
					{
						Key:   "Content-Type",
						Value: []byte("application/json"),
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			c := KafkaClient{w: tc.writter}
			err := c.SendMessage(t.Context(), tc.req)
			if !errors.Is(err, tc.want) {
				t.Fatalf("%s: want: %s, got: %s", tc.name, tc.want, err)
			}
		})
	}
}
