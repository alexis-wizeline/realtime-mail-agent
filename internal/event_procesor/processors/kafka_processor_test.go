package processors_test

import (
	"context"
	"errors"
	"testing"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/clients"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/event_procesor/processors"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/testutils/mocks"
	"github.com/segmentio/kafka-go"
)

func Test_KafkaProcessor_Process(t *testing.T) {
	tcs := []struct {
		name string

		client clients.Client

		want error
	}{
		{
			name:   "base case nil return",
			client: &mocks.MockClient{Err: nil},
			want:   nil,
		},
		{
			name:   "non retry error",
			client: &mocks.MockClient{Err: kafka.InvalidMessage},
			want: processors.ProcessError{
				Err:   kafka.InvalidMessage,
				Retry: false,
			},
		},
		{
			name:   "retry error with Kafka error",
			client: &mocks.MockClient{Err: kafka.BrokerNotAvailable},
			want: processors.ProcessError{
				Err:   kafka.BrokerNotAvailable,
				Retry: true,
			},
		},
		{
			name:   "retry error with non Kafka error",
			client: &mocks.MockClient{Err: context.DeadlineExceeded},
			want: processors.ProcessError{
				Err:   context.DeadlineExceeded,
				Retry: true,
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p := processors.NewKafkaProcessor(tc.client)
			err := p.Process(t.Context(), processors.Job{})
			if !errors.Is(err, tc.want) {
				t.Fatalf("%s: unwanted error want: %s, got: %s", tc.name, tc.want, err)
			}
		})
	}
}
