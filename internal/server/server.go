package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/AbhishekBasnet/goredis/internal/pubsub"
	"github.com/AbhishekBasnet/goredis/internal/resp"
)

type Session interface {
	Dispatch(ctx context.Context, args []string)
	Blocks(args []string) bool
	Subscriber() *pubsub.Subscriber
	Close()
}

type Server struct {
	addr       string
	log        *slog.Logger
	newSession func(w *resp.Writer) Session
}

func New(addr string, log *slog.Logger, newSession func(w *resp.Writer) Session) *Server {
	return &Server{addr: addr, log: log, newSession: newSession}
}

func (s *Server) Run() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	return s.Serve(ln)
}

func (s *Server) Serve(ln net.Listener) error {
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
