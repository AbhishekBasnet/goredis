package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/pubsub"
	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/stats"
)

type Session interface {
	Dispatch(ctx context.Context, args []string)
	Blocks(args []string) bool
	Subscriber() *pubsub.Subscriber
	Close()
}

type AppendLog interface {
	Seq() uint64
	WaitFor(seq uint64) error
}

type Config struct {
	MaxClients    int
	IdleTimeout   time.Duration
	ShutdownGrace time.Duration
	Counters      *stats.Counters
	AppendLog     AppendLog
}

const DefaultShutdownGrace = 5 * time.Second

type Server struct {
	addr       string
	log        *slog.Logger
	newSession func(w *resp.Writer) Session
	cfg        Config

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

func New(addr string, log *slog.Logger, newSession func(w *resp.Writer) Session, cfg Config) *Server {
	if cfg.ShutdownGrace <= 0 {
		cfg.ShutdownGrace = DefaultShutdownGrace
	}
	if cfg.Counters == nil {
		cfg.Counters = &stats.Counters{}
	}
	return &Server{addr: addr, log: log, newSession: newSession, cfg: cfg, conns: make(map[net.Conn]struct{})}
}

func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	return s.Serve(ctx, ln)
}

func (s *Server) Serve(parent context.Context, ln net.Listener) error {
	s.log.Info("listening", "addr", ln.Addr().String())
	ctx, cancel := context.WithCancel(parent)
	context.AfterFunc(ctx, func() { ln.Close() })

	err := s.accept(ctx, ln)
	cancel()
	ln.Close()
	s.drain()
	return err
}

func (s *Server) accept(ctx context.Context, ln net.Listener) error {
	var backoff time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				s.log.Info("shutting down: stopped accepting connections")
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return fmt.Errorf("accept: %w", err)
			}
			backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
			s.log.Warn("accept failed; retrying", "err", err, "in", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
			}
			continue
		}
		backoff = 0

		s.cfg.Counters.TotalConnections.Add(1)
		if !s.track(conn) {
			s.cfg.Counters.RejectedConnections.Add(1)
			s.wg.Go(func() { reject(conn) })
			continue
		}
		s.wg.Go(func() { s.handle(ctx, conn) })
	}
}

func (s *Server) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cfg.MaxClients > 0 && len(s.conns) >= s.cfg.MaxClients {
		return false
	}
	s.conns[conn] = struct{}{}
	s.cfg.Counters.ConnectedClients.Store(int64(len(s.conns)))
	return true
}

func (s *Server) untrack(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.conns, conn)
	s.cfg.Counters.ConnectedClients.Store(int64(len(s.conns)))
}

func reject(conn net.Conn) {
	conn.SetWriteDeadline(time.Now().Add(time.Second))
	conn.Write([]byte("-ERR max number of clients reached\r\n"))
	conn.Close()
}

func (s *Server) drain() {
	s.mu.Lock()
	n := len(s.conns)
	for conn := range s.conns {
		conn.SetReadDeadline(time.Now())
	}
	s.mu.Unlock()
	s.log.Info("draining clients", "clients", n)

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(s.cfg.ShutdownGrace):
		s.mu.Lock()
		s.log.Warn("clients still busy after the grace period; closing them", "clients", len(s.conns))
		for conn := range s.conns {
			conn.Close()
		}
		s.mu.Unlock()
		<-done
	}
	s.log.Info("all clients disconnected")
}
