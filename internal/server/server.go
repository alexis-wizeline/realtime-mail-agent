package server

import (
	"net/http"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
	"github.com/alexis-dragneel/realtime-mail-agent/internal/logger"
)

type Server struct {
	*http.ServeMux
	db  db.DB
	log *logger.Logger
}

func (s *Server) registerPaths() *Server {
	s.Handle("POST /v1/mail-events", handlerIngestEvents(s))

	return s
}

func NewServer(db db.DB, logger *logger.Logger) *Server {
	s := &Server{http.NewServeMux(), db, logger}
	return s.registerPaths()
}
