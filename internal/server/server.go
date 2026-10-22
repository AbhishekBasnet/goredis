package server

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/AbhishekBasnet/goredis/internal/resp"
)

// Dispatcher runs one command and writes its reply. It is defined here,
// where it is used: the server only needs this one method, and
// *command.Registry happens to provide it.
type Dispatcher interface {
	Dispatch(w *resp.Writer, args []string)
}

type Server struct {
	addr string
	log  *slog.Logger
	cmds Dispatcher
}

func New(addr string, log *slog.Logger, cmds Dispatcher) *Server {
	return &Server{addr: addr, log: log, cmds: cmds}
}

// Run listens on the configured address and serves until accepting fails.
func (s *Server) Run() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	return s.Serve(ln)
}

// Serve accepts connections on ln. Tests use it with a listener on port 0,
// so the OS picks a free port.
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
