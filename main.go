package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
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

	port := os.Getenv("PORT")
	server := server.NewServer(db.NewRealtimeMailDB(pool))

	err = http.ListenAndServe(":"+port, server)
	if err != nil {
		log.Fatalf("unable to start server in port: %s, err: %s", port, err)
	}

}
