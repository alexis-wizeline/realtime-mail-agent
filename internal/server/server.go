package server

import (
	"net/http"

	"github.com/alexis-dragneel/realtime-mail-agent/internal/db"
)

type Server struct {
	*http.ServeMux
	db db.DB
}

func (s *Server) registerPaths() *Server {
	s.Handle("POST /v1/mail-events", handlerIngestEvents(s))

	return s
}

func NewServer(db db.DB) *Server {
	s := &Server{http.NewServeMux(), db}
	return s.registerPaths()
}
