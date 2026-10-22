package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/AbhishekBasnet/goredis/internal/resp"
)

type Dispatcher interface {
	Dispatch(ctx context.Context, w *resp.Writer, args []string)
	Blocks(args []string) bool
}

type Server struct {
	addr string
	log  *slog.Logger
	cmds Dispatcher
}

func New(addr string, log *slog.Logger, cmds Dispatcher) *Server {
	return &Server{addr: addr, log: log, cmds: cmds}
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
