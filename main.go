package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/logger"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/server"
	"github.com/subosito/gotenv"
)

func main() {
	err := gotenv.Load()
	if err != nil {
		log.Printf("unable to load .env file: %s", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()
	pool, err := db.SetupDB(ctx)
	if err != nil {
		log.Fatalf("unable to init the db connection pool: %s", err)
	}
	defer pool.Close()

	logger, err := logger.NewLogger("real-time-mail-agent", os.Getenv("LOG_FILE"))
	if err != nil {
		log.Fatalf("Unable to create logger: %s", err)
	}
	defer logger.ShutDown()

	db := db.NewRealtimeMailDB(pool, db.DefaultOutboxMapper)
	server := server.NewServer(db, logger)

	// workerPool, err := eventprocesor.NewEventProcessorPool(ctx, eventprocesor.NewEventProcessorPoolParams{
	// 	WorkerSettings: eventprocesor.ProcessorWorkerPoolSettings{
	// 		Workers:            4,
	// 		EventsWorkerLimit:  10,
	// 		EventLeaseDuration: 10 * time.Minute,
	// 		WorkerInterval:     5 * time.Second,
	// 		WorkerBackoff:      10 * time.Second,
	// 		WorkerJitter:       20 * time.Millisecond,
	// 	},
	// 	DB:        db,
	// 	Processor: nil,
	// 	Logger:    logger,
	// })
	// if err != nil {
	// 	log.Fatalf("unable to create the worker pool: %s", err)
	// }
	// go workerPool.Start()

	port := os.Getenv("PORT")
	err = http.ListenAndServe(":"+port, server)
	if err != nil {
		log.Fatalf("unable to start server in port: %s, err: %s", port, err)
	}

}
