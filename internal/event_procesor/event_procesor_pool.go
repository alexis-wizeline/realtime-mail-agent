package eventprocesor

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/event_procesor/processors"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/logger"
	"github.com/google/uuid"
)

var (
	WorkerPoolSettingsWorkerZeroErr    = errors.New("workers must be at least 1 in the pool settings")
	WorkerPoolEventsWorkerLimitZeroErr = errors.New("the max amount of events for each worker needs to be more than 0")
	WorkerPoolLeaseDurationZeroErr     = errors.New("the lease duration for the worker needs to be more than 0")
	WorkerPoolIntervalZeroErr          = errors.New("the interval for each worker run needs to be more than 0")
	WorkerPoolWorkerBackoffSecZeroErr  = errors.New("the worker backoff needs to be more than 0")
	WorkerPoolLoggerNilErr             = errors.New("the worker pool logger can't be nil")

	PoolDBNilErr        = errors.New("the pool db is null or invalid")
	PoolProcessorNilErr = errors.New("the pool processor is null or invalid")
)

type PoolDB interface {
	ClaimOutboxEvents(context.Context, db.ClaimOutboxEventsParams) ([]realtimemailsql.OutboxEvent, error)
	MarkOutboxEventAsPublished(context.Context, uuid.UUID, uuid.UUID) (bool, error)
	MarkOutboxEventAsFailed(context.Context, db.FailedEventParams) (bool, error)
	MarkOutboxEventAsDiscarded(context.Context, db.DiscardedEventParams) (bool, error)
}

type ProcessorWorkerPoolSettings struct {
	Workers            int
	EventsWorkerLimit  int
	EventLeaseDuration time.Duration

	WorkerInterval time.Duration
	WorkerBackoff  time.Duration
	WorkerJitter   time.Duration
}

func (p ProcessorWorkerPoolSettings) valid() error {
	if p.Workers <= 0 {
		return WorkerPoolSettingsWorkerZeroErr
	}
	if p.EventsWorkerLimit <= 0 {
		return WorkerPoolEventsWorkerLimitZeroErr
	}
	if p.EventLeaseDuration <= 0 {
		return WorkerPoolLeaseDurationZeroErr
	}
	if p.WorkerInterval <= 0 {
		return WorkerPoolIntervalZeroErr
	}
	if p.WorkerBackoff <= 0 {
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

	workers poolWorkers

	pool sync.WaitGroup
	once sync.Once
}

type NewEventProcessorPoolParams struct {
	WorkerSettings ProcessorWorkerPoolSettings
	DB             PoolDB
	Processor      processors.Processor
	Logger         *logger.Logger
}

func (n NewEventProcessorPoolParams) valid() error {
	if n.DB == nil {
		return PoolDBNilErr
	}
	if n.Processor == nil {
		return PoolProcessorNilErr
	}
	if n.Logger == nil {
		return WorkerPoolLoggerNilErr
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
		db:            p.DB,
		processor:     p.Processor,
		logger:        p.Logger,
		eventLimit:    p.WorkerSettings.EventsWorkerLimit,
		leaseDuration: p.WorkerSettings.EventLeaseDuration,
		interval:      p.WorkerSettings.WorkerInterval,
		jitter:        p.WorkerSettings.WorkerJitter,
		backoff:       p.WorkerSettings.WorkerBackoff,
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

		workers: workers,

		pool: sync.WaitGroup{},
	}, nil
}

func (e *EventProcessorPool) Start() {
	e.once.Do(e.startWorkers)
	e.pool.Wait()
}

func (e *EventProcessorPool) startWorkers() {
	for _, ref := range e.workers {
		e.pool.Go(func() {
			ref.worker.work(e.ctx)
		})
	}
}
