package eventprocesor

import (
	"errors"
	"testing"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/event_procesor/processors"
)

func Test_NewEventProcessorPool(t *testing.T) {
	tcs := []struct {
		name   string
		params NewEventProcessorPoolParams

		wantError   bool
		expectedErr error
	}{
		{
			name: "inavlid db",
			params: NewEventProcessorPoolParams{
				DB:        nil,
				Processor: processors.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 5 * 60 * 60,
					WorkerIntervalSec:     2 * 60 * 60,
					WorkerBackoffSec:      1 * 60 * 60,
					WorkerJitter:          10 * 60 / 1000,
				},
			},
			wantError:   true,
			expectedErr: PoolDBNilErr,
		},
		{
			name: "invalid processor",
			params: NewEventProcessorPoolParams{
				DB:        &db.MockDB{},
				Processor: nil,
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 5 * 60 * 60,
					WorkerIntervalSec:     2 * 60 * 60,
					WorkerBackoffSec:      1 * 60 * 60,
					WorkerJitter:          10 * 60 / 1000,
				},
			},
			wantError:   true,
			expectedErr: PoolProcessorNilErr,
		},
		{
			name: "invalid settings worker",
			params: NewEventProcessorPoolParams{
				DB:        &db.MockDB{},
				Processor: processors.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               -1,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 5 * 60 * 60,
					WorkerIntervalSec:     2 * 60 * 60,
					WorkerBackoffSec:      1 * 60 * 60,
					WorkerJitter:          10 * 60 / 1000,
				},
			},
			wantError:   true,
			expectedErr: WorkerPoolSettingsWorkerZeroErr,
		},
		{
			name: "invalid settings workers event limit",
			params: NewEventProcessorPoolParams{
				DB:        &db.MockDB{},
				Processor: processors.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     -1,
					EventLeaseDurationSec: 5 * 60 * 60,
					WorkerIntervalSec:     2 * 60 * 60,
					WorkerBackoffSec:      1 * 60 * 60,
					WorkerJitter:          10 * 60 / 1000,
				},
			},
			wantError:   true,
			expectedErr: WorkerPoolEvenstWorkerLimitZeroErr,
		},
		{
			name: "invalid settings workers lease duration",
			params: NewEventProcessorPoolParams{
				DB:        &db.MockDB{},
				Processor: processors.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 0,
					WorkerIntervalSec:     2 * 60 * 60,
					WorkerBackoffSec:      1 * 60 * 60,
					WorkerJitter:          10 * 60 / 1000,
				},
			},
			wantError:   true,
			expectedErr: WorkerPoolLeaseDurationZeroErr,
		},
		{
			name: "invalid settings workers interval duration",
			params: NewEventProcessorPoolParams{
				DB:        &db.MockDB{},
				Processor: processors.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 5 * 60 * 60,
					WorkerIntervalSec:     0,
					WorkerBackoffSec:      1 * 60 * 60,
					WorkerJitter:          10 * 60 / 1000,
				},
			},
			wantError:   true,
			expectedErr: WorkerPoolIntervalZeroErr,
		},
		{
			name: "invalid settings workers backoff duration",
			params: NewEventProcessorPoolParams{
				DB:        &db.MockDB{},
				Processor: processors.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 5 * 60 * 60,
					WorkerIntervalSec:     2 * 60 * 60,
					WorkerBackoffSec:      0,
					WorkerJitter:          10 * 60 / 1000,
				},
			},
			wantError:   true,
			expectedErr: WorkerPoolWorkerBackoffSecZeroErr,
		},
		{
			name: "invalid worker lease lower than interval",
			params: NewEventProcessorPoolParams{
				DB:        &db.MockDB{},
				Processor: processors.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 5 * 60 * 60,
					WorkerIntervalSec:     20 * 60 * 60,
					WorkerBackoffSec:      2 * 60 * 60,
					WorkerJitter:          10 * 60 * 60 / 1000,
				},
			},
			wantError:   true,
			expectedErr: WorkerLeaseDurationLowerIntervalSecErr,
		},
		{
			name: "valid params",
			params: NewEventProcessorPoolParams{
				DB:        &db.MockDB{},
				Processor: processors.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 5 * 60 * 60,
					WorkerIntervalSec:     2 * 60 * 60,
					WorkerBackoffSec:      2 * 60 * 60,
					WorkerJitter:          10 * 60 * 60 / 1000,
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {

			pool, err := NewEventProcessorPool(t.Context(), tc.params)
			if err != nil && !tc.wantError {
				t.Fatalf("bad params in test: %s, err: %s", tc.name, err.Error())
			}

			if tc.wantError {
				if !errors.Is(err, tc.expectedErr) {
					t.Fatalf("unexpected error happen on %s, want: %s, got: %s", tc.name, tc.expectedErr.Error(), err.Error())
				}
				return
			}

			if len(pool.workers) != tc.params.WorkerSettings.Workers {
				t.Fatalf("Fail: %s, expected workers: %d, got: %d", tc.name, tc.params.WorkerSettings.Workers, len(pool.workers))
			}

			for _, ref := range pool.workers {
				if ref.status != pending {
					t.Fatalf("Failed: %s, worker bad initializied with status: %s", tc.name, ref.status)
				}
			}

		})
	}
}
