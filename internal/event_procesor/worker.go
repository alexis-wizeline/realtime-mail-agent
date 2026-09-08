package eventprocesor

import (
	"context"
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

type worker struct {
	id uuid.UUID

	db         processorDB
	p          processors.Processor
	eventLimit int

	intervalSec int64
	jitter      int64
}

func (w *worker) work(ctx context.Context) {
	for {
		// get jobs
		events, err := w.db.ClaimOutboxEvents(ctx, db.ClaimOutboxEventsParams{
			WorkerName:  w.id.String(),
			JobsLimit:   int32(w.eventLimit),
			LockedUntil: time.Now().Add(time.Duration(w.intervalSec) * time.Second),
		})
		// uh? what to do here ? err not nil log the error then what ?
		if err != nil {
			slog.Error("worker failed to get jobs", "worker_id", w.id, "error", err)
		}
		// 2 and 3
		w.handleEvents(ctx, events)
		select {
		case <-time.After(w.nextIterationAt()):
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
		err := w.p.Process(ctx, event)
		if err != nil {
			// mark as failed
			continue
		}
		// mark as published
	}

}

func (w *worker) nextIterationAt() time.Duration {
	max := time.Duration(w.intervalSec+w.jitter) * time.Second
	r := rand.New(rand.NewSource(time.Now().Unix()))
	return time.Duration(r.Int63n(int64(max)))
}
