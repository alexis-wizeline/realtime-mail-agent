package clients

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

type fackeWritter struct {
	err error
}

func (f *fackeWritter) Close() error {
	return nil
}

func (f *fackeWritter) WriteMessages(context.Context, ...kafka.Message) error {
	return f.err
}

func Test_kafkaClient_error(t *testing.T) {
	wildError := errors.New("a wild error has appear")

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
			writter: &fackeWritter{err: EmptyTopicErr},
			req:     MessageRequest{Message: []byte("hi")},
			want:    EmptyTopicErr,
		},
		{
			name:    "empty message",
			writter: &fackeWritter{err: EmptyMessageErr},
			req:     MessageRequest{Topic: "events"},
			want:    EmptyMessageErr,
		},
		{
			name:    "error is propagated",
			writter: &fackeWritter{err: wildError},
			req:     MessageRequest{Topic: "events", Message: []byte("hi")},
			want:    wildError,
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
