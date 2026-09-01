package server

import (
	"log"
	"net/http"

	ingestevents "github.com/alexis-dragneel/realtime-mail-agent/internal/server/models/ingest_events"
)

type serverHandler func(*Server) http.HandlerFunc

var handlerIngestEvents serverHandler = func(s *Server) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		event, err := ingestevents.IngestEVentReqFromBodyRequest(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err = s.db.CreateEvents(r.Context(), event)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Printf("error while creating events: %s", err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
}
