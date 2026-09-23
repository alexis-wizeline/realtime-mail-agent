package eventprocesor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/event_procesor/processors"
	testingutils "github.com/alexis-dragneel/realtime-mail-agent/internal/testutils"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/testutils/mocks"
)

func Test_NewEventProcessorPool(t *testing.T) {
	tcs := []struct {
		name   string
		params NewEventProcessorPoolParams

		wantError   bool
		expectedErr error
	}{
		{
			name: "invalid db",
			params: NewEventProcessorPoolParams{
				DB:        nil,
				Processor: mocks.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 1 * 60 * 60,
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
				DB:        &mocks.MockDB{},
				Processor: nil,
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 1 * 60 * 60,
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
				DB:        &mocks.MockDB{},
				Processor: mocks.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               -1,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 1 * 60 * 60,
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
				DB:        &mocks.MockDB{},
				Processor: mocks.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     -1,
					EventLeaseDurationSec: 1 * 60 * 60,
					WorkerIntervalSec:     2 * 60 * 60,
					WorkerBackoffSec:      1 * 60 * 60,
					WorkerJitter:          10 * 60 / 1000,
				},
			},
			wantError:   true,
			expectedErr: WorkerPoolEventsWorkerLimitZeroErr,
		},
		{
			name: "invalid settings workers lease duration",
			params: NewEventProcessorPoolParams{
				DB:        &mocks.MockDB{},
				Processor: mocks.NewMockProcess(nil),
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
				DB:        &mocks.MockDB{},
				Processor: mocks.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 1 * 60 * 60,
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
				DB:        &mocks.MockDB{},
				Processor: mocks.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 1 * 60 * 60,
					WorkerIntervalSec:     2 * 60 * 60,
					WorkerBackoffSec:      0,
					WorkerJitter:          10 * 60 / 1000,
				},
			},
			wantError:   true,
			expectedErr: WorkerPoolWorkerBackoffSecZeroErr,
		},
		{
			name: "invalid worker lease higher than 1 hour",
			params: NewEventProcessorPoolParams{
				DB:        &mocks.MockDB{},
				Processor: mocks.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 5 * 60 * 60,
					WorkerIntervalSec:     2 * 60 * 60,
					WorkerBackoffSec:      2 * 60 * 60,
					WorkerJitter:          10 * 60 * 60 / 1000,
				},
			},
			wantError:   true,
			expectedErr: WorkerLeaseDurationHigherThanMAxLeaseErr,
		},
		{
			name: "valid params",
			params: NewEventProcessorPoolParams{
				DB:        &mocks.MockDB{},
				Processor: mocks.NewMockProcess(nil),
				WorkerSettings: ProcessorWorkerPoolSettings{
					Workers:               2,
					EventsWorkerLimit:     10,
					EventLeaseDurationSec: 1 * 60 * 60,
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
		})
	}
}

func Test_EventPool_Start_Process_Events(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	utilsDB := testingutils.NewTestutilsDB(ctx, t)
	defer utilsDB.Done()

	eventsQuantity := 20
	var counter atomic.Uint64
	doneCH := make(chan struct{})
	p := mocks.NewMockProcess(func(_ context.Context, _ processors.Job) error {
		counter.Add(1)
		if counter.Load() >= uint64(eventsQuantity) {
			doneCH <- struct{}{}
		}
		return nil
	})

	eventIDs, err := utilsDB.AddOutboxEvents(ctx, eventsQuantity)
	if err != nil {
		t.Fatalf("unable to create events: %v", err)
	}

	breakCH, errCh := make(chan bool, 1), make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-doneCH:
				for {
					events, err := utilsDB.QueryEventStatuses(t.Context(), eventIDs)
					if err != nil {
						errCh <- err
						breakCH <- true
					}

					index := foundIvalidStatusIndex(events, "published")
					if index == -1 {
						done()
						return
					}
				}
			}
		}
	}()

	go func() {
		select {
		case b := <-breakCH:
			if b {
				done()
				return
			}
		case <-ctx.Done():
			return
		}
	}()

	pool, err := NewEventProcessorPool(ctx, NewEventProcessorPoolParams{
		DB:        utilsDB.DB,
		Processor: p,
		WorkerSettings: ProcessorWorkerPoolSettings{
			Workers:               4,
			EventsWorkerLimit:     5,
			EventLeaseDurationSec: 60,
			WorkerIntervalSec:     5,
			WorkerBackoffSec:      2,
			WorkerJitter:          2,
		},
	})
	if err != nil {
		t.Fatalf("unable to initialize pool: %v", err)
	}
	pool.Start()
	close(breakCH)
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("error while processing events: %v", err)
		}
	}

	events, err := utilsDB.QueryEventStatuses(t.Context(), eventIDs)
	if err != nil {
		t.Fatalf("unable to query the processed events: %v", err)
	}

	if len(events) != eventsQuantity {
		t.Fatalf("num events found not match the test want: %v, got: %v", eventsQuantity, len(events))
	}

	if invalidIndex := foundIvalidStatusIndex(events, "published"); invalidIndex >= 0 {
		t.Fatalf("Event: %v is not in in published status, is on %s", events[invalidIndex].ID, events[invalidIndex].Status)
	}

	err = utilsDB.CleanupDB(t.Context())
	if err != nil {
		t.Fatalf("cleanup test failed: %v", err)
	}
}

func Test_eventPool_start_Ctx_cancelled_Cancel_Operations(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	utilsDB := testingutils.NewTestutilsDB(ctx, t)
	defer utilsDB.Done()

	doneCh := make(chan struct{})
	var counter atomic.Int32
	p := mocks.NewMockProcess(func(ctx context.Context, _ processors.Job) error {
		counter.Add(1)
		doneCh <- struct{}{}
		<-ctx.Done()
		return context.Canceled
	})

	go func() {
		<-doneCh
		done()
	}()

	eventIDs, err := utilsDB.AddOutboxEvents(ctx, 1)
	if err != nil {
		t.Fatalf("unable to create events: %v", err)
	}

	pool, err := NewEventProcessorPool(ctx, NewEventProcessorPoolParams{
		DB:        utilsDB.DB,
		Processor: p,
		WorkerSettings: ProcessorWorkerPoolSettings{
			Workers:               1,
			EventsWorkerLimit:     5,
			EventLeaseDurationSec: 60,
			WorkerIntervalSec:     5,
			WorkerBackoffSec:      2,
			WorkerJitter:          2,
		},
	})
	if err != nil {
		t.Fatalf("unable to initialize pool: %v", err)
	}
	pool.Start()

	events, err := utilsDB.QueryEventStatuses(t.Context(), eventIDs)
	if err != nil {
		t.Fatalf("unable to query the processed events: %v", err)
	}

	if len(events) != 1 {
		t.Fatalf("num events found not match the test want: %v, got: %v", 1, len(events))
	}

	if invalidIndex := foundIvalidStatusIndex(events, "processing"); invalidIndex >= 0 {
		t.Fatalf("Event: %v is not in in published status, is on %s", events[invalidIndex].ID, events[invalidIndex].Status)
	}

	err = utilsDB.CleanupDB(t.Context())
	if err != nil {
		t.Fatalf("cleanup test failed: %v", err)
	}

	workerSet := make(map[string]struct{})
	for _, e := range events {

		if !e.AssignedWorkerName.Valid {
			continue
		}
		if _, ok := workerSet[e.AssignedWorkerName.String]; ok {
			continue
		}

		workerSet[e.AssignedWorkerName.String] = struct{}{}
	}

	for _, ref := range pool.workers {
		_, ok := workerSet[ref.worker.key().String()]
		if !ok {
			t.Fatalf("worker id: %s, not found in the stoped events", ref.worker.key())
		}
	}

}

func foundIvalidStatusIndex(evs []testingutils.TestutilsOutboxEvent, status string) int {
	for i, ev := range evs {
		if ev.Status != status {
			return i
		}
	}
	return -1
}
