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
	newtWorkBackoffSec   = 120
	nextAttempBackoffSec = 60
)

type worker struct {
	id uuid.UUID

	db processorDB
	p  processors.Processor

	eventLimit       int
	leaseDurationSec int

	intervalSec          int64
	jitter               int64
	backOffSec           int64
	maxNetworkErrorRetry int64
}

func (w *worker) work(ctx context.Context) {

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
			default:
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

func (w *worker) handleEvents(ctx context.Context, events []realtimemailsql.OutboxEvent) {
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
		err := w.p.Process(ctx, job)
		if err != nil {
			w.failure(ctx, job, err, true)
			continue
		}
		w.success(ctx, event.ID.Bytes)
	}
}

// TODO: change slog for w.log
func (w *worker) success(ctx context.Context, eventID uuid.UUID) {
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
func (w *worker) failure(ctx context.Context, job processors.Job, err error, retry bool) {
	if errors.Is(err, processors.RetriableError) && retry {
		w.retry(ctx, job)
		return
	}

	marked, err := w.db.MarkOutboxEventAsFailed(ctx, db.MarkOutboxEventAsFailedParams{
		EventID:      job.ID,
		WorkerID:     w.id,
		Err:          err,
		NextAttempAt: time.Now().Add(nextAttempBackoffSec * time.Second),
	})
	if err != nil {
		slog.Error("failure: failed in the db", "event_id", job.ID, "error", err)
		return
	}
	if !marked {
		slog.Error("failure: failed event not marked", "event_id", job.ID)
		return
	}
	slog.Info("failure: event marked", "event_id", job.ID)
}

// TODO: change slog for w.log
func (w *worker) retry(ctx context.Context, job processors.Job) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for attemps := 0; attemps < int(w.maxNetworkErrorRetry); attemps++ {
		timer.Reset(w.retryBackoff(attemps))
		select {
		case <-timer.C:
		case <-ctx.Done():
			slog.Info("retry: cancelled", "event_id", job.ID)
			return
		}

		err := w.p.Process(ctx, job)
		if err != nil {
			if !errors.Is(err, processors.RetriableError) || attemps == int(w.maxNetworkErrorRetry)-1 {
				w.failure(ctx, job, err, false)
				break
			}
			slog.Error("retry: failed", "event_id", job.ID, "attemp", attemps, "max_attemps", w.maxNetworkErrorRetry)
			continue
		}
		w.success(ctx, job.ID)
		break
	}
}

func (w *worker) nextIterationAt() time.Duration {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	next := (w.intervalSec + r.Int63n(int64(w.jitter))) * int64(time.Second)
	return time.Duration(next)
}

func (w *worker) retryBackoff(attemp int) time.Duration {
	base := 100 * time.Millisecond
	max := time.Duration(w.backOffSec) * time.Second

	duration := time.Duration(1<<attemp) * base
	if duration > max {
		return max
	}

	return duration
}
