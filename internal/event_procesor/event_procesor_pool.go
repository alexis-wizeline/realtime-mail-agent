package eventprocesor

import (
	"context"
	"errors"
	"sync"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/event_procesor/processors"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	"github.com/google/uuid"
)

var (
	WorkerPoolSettingsWorkerZeroErr    = errors.New("workers must be at least 1 in the pool settings")
	WorkerPoolEvenstWorkerLimitZeroErr = errors.New("the max amount of events for each worker needs to be more than 0")
	WorkerPoolLeaseDurationZeroErr     = errors.New("the lease duration for the worker needs to be more than 0")
	WorkerPoolIntervalZeroErr          = errors.New("the interval for each worker run needs to be more than 0")
	WorkerPoolWorkerBackoffSecZeroErr  = errors.New("the worker backoff needs to be more than 0")

	PoolDBNilErr        = errors.New("the pool db is null or invalid")
	PoolProcessorNilErr = errors.New("the pool processor os null or invalid")
)

type PoolDB interface {
	ClaimOutboxEvents(context.Context, db.ClaimOutboxEventsParams) ([]realtimemailsql.OutboxEvent, error)
	MarkOutboxEventAsPublished(context.Context, uuid.UUID, uuid.UUID) (bool, error)
	MarkOutboxEventAsFailed(context.Context, db.FailedEventParams) (bool, error)
	MarkOutboxEventAsDiscarded(context.Context, db.DiscardedEventParams) (bool, error)
}

type ProcessorWorkerPoolSettings struct {
	Workers               int
	EventsWorkerLimit     int
	EventLeaseDurationSec int64

	WorkerIntervalSec int64
	WorkerBackoffSec  int64
	WorkerJitter      int64
}

func (p ProcessorWorkerPoolSettings) valid() error {
	if p.Workers <= 0 {
		return WorkerPoolSettingsWorkerZeroErr
	}
	if p.EventsWorkerLimit <= 0 {
		return WorkerPoolEvenstWorkerLimitZeroErr
	}
	if p.EventLeaseDurationSec <= 0 {
		return WorkerPoolLeaseDurationZeroErr
	}
	if p.WorkerIntervalSec <= 0 {
		return WorkerPoolIntervalZeroErr
	}
	if p.WorkerBackoffSec <= 0 {
		return WorkerPoolWorkerBackoffSecZeroErr
	}

	return nil
}

type workerRef struct {
	worker worker
}
type poolWorkers map[uuid.UUID]*workerRef

type EventProcessorPool struct {
	ctx context.Context

	// ref to future recovery of jobs
	settings  ProcessorWorkerPoolSettings
	db        PoolDB
	processor processors.Processor

	workers poolWorkers

	pool sync.WaitGroup
	init bool
}

type NewEventProcessorPoolParams struct {
	WorkerSettings ProcessorWorkerPoolSettings
	DB             PoolDB
	Processor      processors.Processor
}

func (n NewEventProcessorPoolParams) valid() error {
	if n.DB == nil {
		return PoolDBNilErr
	}
	if n.Processor == nil {
		return PoolProcessorNilErr
	}
	if err := n.WorkerSettings.valid(); err != nil {
		return err
	}
	return nil
}

func NewEventProcessorPool(ctx context.Context, p NewEventProcessorPoolParams) (*EventProcessorPool, error) {
	err := p.valid()
	if err != nil {
		return nil, err
	}
	workerSettings := workerEventSettings{
		db:               p.DB,
		processor:        p.Processor,
		eventLimit:       p.WorkerSettings.EventsWorkerLimit,
		leaseDurationSec: p.WorkerSettings.EventLeaseDurationSec,
		intervalSec:      p.WorkerSettings.WorkerIntervalSec,
		jitter:           p.WorkerSettings.WorkerJitter,
		backoffSec:       p.WorkerSettings.WorkerBackoffSec,
	}
	workers := make(poolWorkers)
	for _ = range p.WorkerSettings.Workers {
		w, err := newEventWorker(workerSettings)
		if err != nil {
			return nil, err
		}
		workers[w.key()] = &workerRef{
			worker: w,
		}
	}
	return &EventProcessorPool{
		ctx: ctx,

		db:        p.DB,
		processor: p.Processor,
		settings:  p.WorkerSettings,

		workers: workers,

		pool: sync.WaitGroup{},
	}, nil
}

func (e *EventProcessorPool) Start() {
	if e.init {
		return
	}
	e.init = true
	e.startWorkers()
	e.pool.Wait()
}

func (e *EventProcessorPool) startWorkers() {
	for _, ref := range e.workers {
		e.pool.Go(func() {
			ref.worker.work(e.ctx)
		})
	}
}
