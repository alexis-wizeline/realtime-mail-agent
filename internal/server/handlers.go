package server

import (
	"log"
	"net/http"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/server/models"
)

type serverHandler func(*Server) http.HandlerFunc

var handlerIngestEvents = func(s *Server) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		event, err := models.IngestEVentReqFromBodyRequest(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		err = s.db.CreateEvents(r.Context(), event)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Printf("erro while creating events: %s", err)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
}
