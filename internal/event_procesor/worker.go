package eventprocesor

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"time"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/event_procesor/processors"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/generated/realtimemailsql"
	"github.com/google/uuid"
)

////
// worker
// id (UUID) - identifier bets if is unique
//
// db - it would call to get the amount of jobs to process
// processor - and interface that contains the logic to process the jobs
//
// eventLimit <- the number of jobs to process each run
//
// intervalSec - every time the process should work
// jitter - to spread the runs and avoid call ovehead to the db
//
// fields that are to consider
// statusCh bool - to report to the pool that the job is still working
// ???
//
//
// What a worker does?
// first pass
// work(context) <- do the work
// 		1.- get jobs
//      2.- call processor.Process(job) get an err
//      3.- err null? no - send to published, yes - sedn to failed with err (probably another PR to make batch queries into single job query?)
// future consideration?
// start(ctx) <- init backfround jobs for work(ctx) and beat()
// 		work(ctx) <- same as before
// 		beat() <- reports to the pool that is still alive so it can refresh unfinished jobs
// 			- single action after interval statusCh<-true but biggest question how should handle dead?
//
// to handle retries in the process we should do go processor.process(ctx, e) and report throuhg a channel succes or failure proabbaly 2 chanels not ablocker in a first iteration
//

const (
	nextAttempBackoffSec = 60
	defaultJitter        = 20
)

var (
	WorkerDBNilErr             = errors.New("the database for the worker can not be nil")
	WorkerProcessorNilErr      = errors.New("the processor for the worker can not be nil")
	WorkerEventLimitZeroErr    = errors.New("the event limit for the worker must be higher than zero")
	WorkerLeaseDurationZeroErr = errors.New("the lease duration for the worker must be higher than zero")
	WorkerIntervalSecZeroErr   = errors.New("the interval for the worker must be higher than zero")
	WorkerBackOffSecZeroErr    = errors.New("the backoff retry for the worker needs to be higher than zero")
)

type worker interface {
	work(context.Context)
}

type eventWorker struct {
	id uuid.UUID

	db        processorDB
	processor processors.Processor

	eventLimit       int
	leaseDurationSec int

	intervalSec int64
	jitter      int64
	backOffSec  int64
}

type workerEventSettings struct {
	db        processorDB
	processor processors.Processor

	eventLimit       int
	leaseDurationSec int

	intervalSec int64
	jitter      int64
	backoffSec  int64
}

func (w workerEventSettings) valid() error {
	if w.db == nil {
		return WorkerDBNilErr
	}
	if w.processor == nil {
		return WorkerProcessorNilErr
	}
	if w.eventLimit <= 0 {
		return WorkerEventLimitZeroErr
	}
	if w.leaseDurationSec <= 0 {
		return WorkerLeaseDurationZeroErr
	}
	if w.intervalSec <= 0 {
		return WorkerIntervalSecZeroErr
	}
	if w.backoffSec <= 0 {
		return WorkerBackOffSecZeroErr
	}
	return nil
}

func newEventWorker(s workerEventSettings) (worker, error) {
	err := s.valid()
	if err != nil {
		return nil, err
	}
	jiter := s.jitter
	if jiter <= 0 {
		jiter = defaultJitter
	}
	return &eventWorker{
		id: uuid.New(),

		db:        s.db,
		processor: s.processor,

		eventLimit:       s.eventLimit,
		leaseDurationSec: s.leaseDurationSec,

		intervalSec: s.intervalSec,
		jitter:      jiter,
		backOffSec:  s.backoffSec,
	}, nil
}

func (w *eventWorker) work(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	for {
		// get jobs
		events, err := w.db.ClaimOutboxEvents(ctx, db.ClaimOutboxEventsParams{
			WorkerName:  w.id.String(),
			JobsLimit:   int32(w.eventLimit),
			LockedUntil: time.Now().Add(time.Duration(w.leaseDurationSec) * time.Second),
		})
		if err != nil || len(events) == 0 {
			if err != nil {
				// TODO once we have a logger we use w.logger instead
				slog.Error("worker failed to get jobs", "worker_id", w.id, "error", err)
			}

			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			timer.Reset(time.Duration(w.backOffSec) * time.Second)

			select {
			case <-timer.C:
				continue
			case <-ctx.Done():
				return
			}
		}
		w.handleEvents(ctx, events)

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		timer.Reset(w.nextIterationAt())

		select {
		case <-timer.C:
		case <-ctx.Done():
			return
		}
	}
}

func (w *eventWorker) handleEvents(ctx context.Context, events []realtimemailsql.OutboxEvent) {
	if len(events) == 0 {
		return
	}

	for _, event := range events {
		job := processors.Job{
			ID:        event.ID.Bytes,
			EventType: event.EventType,
			Topic:     event.Topic,
			Payload:   event.Payload,
		}
		err := w.processor.Process(ctx, job)
		if err != nil {
			w.failure(ctx, event, err)
			continue
		}
		w.success(ctx, event.ID.Bytes)
	}
}

// TODO: change slog for w.log
func (w *eventWorker) success(ctx context.Context, eventID uuid.UUID) {
	marked, err := w.db.MarkOutboxEventAsPublished(ctx, eventID, w.id)
	if err != nil {
		slog.Error("success: failed for db error", "event_id", eventID, "error", err)
		return
	}
	if !marked {
		slog.Error("success: failed event db record not changed", "event_id", eventID)
		return
	}
	slog.Info("success: event updated", "event_id", eventID)
}

// TODO: change slog for w.log
func (w *eventWorker) failure(ctx context.Context, event realtimemailsql.OutboxEvent, err error) {
	var marked bool
	var queryErr error
	if retryEvent(event, err) {
		marked, queryErr = w.db.MarkOutboxEventAsFailed(ctx, db.FailedEventParams{
			EventID:      event.ID.Bytes,
			WorkerID:     w.id,
			Err:          err,
			NextAttempAt: time.Now().Add(nextAttempBackoffSec * time.Second),
		})
	} else {
		marked, queryErr = w.db.MarkOutboxEventAsDiscarded(ctx, db.DiscardedEventParams{
			EventID:  event.ID.Bytes,
			WorkerID: w.id,
			Err:      err,
		})
	}

	if queryErr != nil {
		slog.Error("failure: failed in the db", "event_id", event.ID, "error", queryErr)
		return
	}
	if !marked {
		slog.Error("failure: failed event not marked", "event_id", event.ID)
		return
	}
	slog.Info("failure: event marked", "event_id", event.ID)
}

func (w *eventWorker) nextIterationAt() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	next := (w.intervalSec + r.Int63n(int64(w.jitter))) * int64(time.Second)
	return time.Duration(next)
}

func retryEvent(e realtimemailsql.OutboxEvent, err error) bool {
	var processError processors.ProcessError
	if errors.As(err, &processError) &&
		processError.Retriable() &&
		e.Attempts < e.MaxAttempts {
		return true
	}

	return false
}
