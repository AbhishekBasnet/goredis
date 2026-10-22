package server

import (
	"fmt"
	"log/slog"
	"net"
)

type Server struct {
	addr string
	log  *slog.Logger
}

func New(addr string, log *slog.Logger) *Server {
	return &Server{addr: addr, log: log}
}

func (s *Server) Run() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	defer ln.Close()

	s.log.Info("listening", "addr", ln.Addr().String())

	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("accept: %w", err)
		}
		go s.handle(conn)
	}

}
