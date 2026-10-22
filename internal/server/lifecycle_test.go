package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/pubsub"
	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/stats"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

type running struct {
	addr     string
	stop     context.CancelFunc
	finished chan struct{}
	err      error
	counters *stats.Counters
}

func serve(t *testing.T, cfg Config, newSession func(w *resp.Writer) Session) *running {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Counters == nil {
		cfg.Counters = &stats.Counters{}
	}
	if newSession == nil {
		reg := newRegistry(store.New())
		newSession = func(w *resp.Writer) Session { return reg.NewClient(w) }
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{addr: ln.Addr().String(), stop: cancel, finished: make(chan struct{}), counters: cfg.Counters}
	srv := New("", slog.New(slog.DiscardHandler), newSession, cfg)
	go func() {
		r.err = srv.Serve(ctx, ln)
		close(r.finished)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.finished:
		case <-time.After(10 * time.Second):
			t.Error("Serve never returned")
		}
	})
	return r
}

func (r *running) shutdown(t *testing.T, within time.Duration) {
	t.Helper()
	r.stop()
	select {
	case <-r.finished:
		if r.err != nil {
			t.Fatalf("Serve returned %v", r.err)
		}
	case <-time.After(within):
		t.Fatalf("Serve did not return within %v", within)
	}
}

func dialServer(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	t.Cleanup(func() { c.Close() })
	return c
}

func expectClosed(t *testing.T, c net.Conn) {
	t.Helper()
	buf := make([]byte, 64)
	n, err := c.Read(buf)
	if n > 0 {
		t.Fatalf("expected the connection to close, got %q", buf[:n])
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestShutdownWakesEveryKindOfClient(t *testing.T) {
	r := serve(t, Config{}, nil)
	idle := dialServer(t, r.addr)
	blocked := dialServer(t, r.addr)
	sub := dialServer(t, r.addr)

	send(idle, "PING\r\n")
	readExactly(t, idle, "+PONG\r\n")
	send(sub, "SUBSCRIBE news\r\n")
	readExactly(t, sub, "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n")
	send(blocked, "PING\r\nBLPOP jobs 0\r\n")
	readExactly(t, blocked, "+PONG\r\n")
	time.Sleep(50 * time.Millisecond)

	r.shutdown(t, 2*time.Second)

	expectClosed(t, idle)
	readExactly(t, blocked, "*-1\r\n")
	expectClosed(t, blocked)
	expectClosed(t, sub)
	if _, err := net.Dial("tcp", r.addr); err == nil {
		t.Error("server still accepts connections after shutdown")
	}
}

type slowSession struct {
	w           *resp.Writer
	started     chan struct{}
	release     chan struct{}
	once        sync.Once
	releaseOnce sync.Once
}

func newSlowSession() *slowSession {
	return &slowSession{started: make(chan struct{}), release: make(chan struct{})}
}

func (s *slowSession) finish() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func (s *slowSession) Dispatch(ctx context.Context, args []string) {
	s.once.Do(func() { close(s.started) })
	<-s.release
	s.w.WriteSimple("DONE")
}
func (s *slowSession) Blocks([]string) bool           { return false }
func (s *slowSession) Subscriber() *pubsub.Subscriber { return nil }
func (s *slowSession) Close()                         {}

func TestShutdownFinishesInFlightCommand(t *testing.T) {
	sess := newSlowSession()
	defer sess.finish()
	r := serve(t, Config{}, func(w *resp.Writer) Session { sess.w = w; return sess })
	c := dialServer(t, r.addr)

	send(c, "WORK\r\n")
	<-sess.started
	r.stop()

	select {
	case <-r.finished:
		t.Fatal("Serve returned while a command was still running")
	case <-time.After(100 * time.Millisecond):
	}

	sess.finish()
	readExactly(t, c, "+DONE\r\n")
	expectClosed(t, c)
	select {
	case <-r.finished:
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after the command finished")
	}
}

func TestShutdownGraceClosesStuckClients(t *testing.T) {
	sess := newSlowSession()
	defer sess.finish()
	r := serve(t, Config{ShutdownGrace: 100 * time.Millisecond}, func(w *resp.Writer) Session { sess.w = w; return sess })
	c := dialServer(t, r.addr)

	send(c, "WORK\r\n")
	<-sess.started
	start := time.Now()
	r.stop()

	expectClosed(t, c)
	if d := time.Since(start); d < 100*time.Millisecond || d > 2*time.Second {
		t.Errorf("stuck client closed after %v, want about the 100ms grace", d)
	}
}

func TestMaxClients(t *testing.T) {
	r := serve(t, Config{MaxClients: 2}, nil)
	a, b := dialServer(t, r.addr), dialServer(t, r.addr)
	send(a, "PING\r\n")
	readExactly(t, a, "+PONG\r\n")
	send(b, "PING\r\n")
	readExactly(t, b, "+PONG\r\n")

	extra := dialServer(t, r.addr)
	readExactly(t, extra, "-ERR max number of clients reached\r\n")
	expectClosed(t, extra)
	if n := r.counters.RejectedConnections.Load(); n != 1 {
		t.Errorf("rejected = %d, want 1", n)
	}

	a.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c := dialServer(t, r.addr)
		send(c, "PING\r\n")
		reply := make([]byte, 7)
		io.ReadFull(c, reply)
		if string(reply) == "+PONG\r\n" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("a freed slot was never reused")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestIdleTimeout(t *testing.T) {
	r := serve(t, Config{IdleTimeout: 150 * time.Millisecond}, nil)
	idle := dialServer(t, r.addr)
	active := dialServer(t, r.addr)
	sub := dialServer(t, r.addr)
	blocked := dialServer(t, r.addr)

	send(sub, "SUBSCRIBE c\r\n")
	readExactly(t, sub, "*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n")
	send(blocked, "BLPOP q 0.5\r\n")
	send(idle, "PING\r\n")
	readExactly(t, idle, "+PONG\r\n")

	start := time.Now()
	for time.Since(start) < 400*time.Millisecond {
		send(active, "PING\r\n")
		readExactly(t, active, "+PONG\r\n")
		time.Sleep(50 * time.Millisecond)
	}
	expectClosed(t, idle)

	readExactly(t, blocked, "*-1\r\n")
	if d := time.Since(start); d < 450*time.Millisecond {
		t.Errorf("BLPOP returned after %v: the idle timeout cut it short", d)
	}
	send(sub, "PING\r\n")
	readExactly(t, sub, "*2\r\n$4\r\npong\r\n$0\r\n\r\n")
	send(active, "PING\r\n")
	readExactly(t, active, "+PONG\r\n")
}

func TestConnectionCounters(t *testing.T) {
	r := serve(t, Config{}, nil)
	a, b := dialServer(t, r.addr), dialServer(t, r.addr)
	for _, c := range []net.Conn{a, b} {
		send(c, "PING\r\n")
		readExactly(t, c, "+PONG\r\n")
	}
	if n := r.counters.ConnectedClients.Load(); n != 2 {
		t.Errorf("connected = %d, want 2", n)
	}

	a.Close()
	deadline := time.Now().Add(time.Second)
	for r.counters.ConnectedClients.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("connected = %d after a disconnect, want 1", r.counters.ConnectedClients.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n := r.counters.TotalConnections.Load(); n != 2 {
		t.Errorf("total = %d, want 2", n)
	}
}

func TestNoGoroutineLeaks(t *testing.T) {
	before := runtime.NumGoroutine()

	r := serve(t, Config{}, nil)
	for range 30 {
		c := dialServer(t, r.addr)
		send(c, "PING\r\nSUBSCRIBE c\r\n")
		readExactly(t, c, "+PONG\r\n*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n")
		c.Close()

		b := dialServer(t, r.addr)
		send(b, "PING\r\nBLPOP q 0\r\n")
		readExactly(t, b, "+PONG\r\n")
		b.Close()
	}
	idle := dialServer(t, r.addr)
	send(idle, "PING\r\n")
	readExactly(t, idle, "+PONG\r\n")
	r.shutdown(t, 2*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("%d goroutines before, %d after:\n%s", before, runtime.NumGoroutine(), buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}
