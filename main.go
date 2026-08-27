package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/server"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/subosito/gotenv"
)

func main() {
	err := gotenv.Load()
	if err != nil {
		log.Fatalf("unable to load .env file: %s", err)
	}

	pool, err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("unable to connect to the db err: %s", err)
	}
	defer pool.Close()

	port := os.Getenv("PORT")
	server := server.NewServer(db.NewRealtimeMailDB(pool))

	err = http.ListenAndServe(":"+port, server)
	if err != nil {
		log.Fatalf("unable to start server in port: %s, err: %s", port, err)
	}

}
