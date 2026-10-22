package server

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/pubsub"
	"github.com/AbhishekBasnet/goredis/internal/resp"
)

func (s *Server) handle(conn net.Conn) {
	remote := conn.RemoteAddr().String()
	s.log.Info("Client connected", "remote", remote)
	defer s.log.Info("Client disconnected", "remote", remote)

	ctx := context.Background()

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	var wmu sync.Mutex
	sess := s.newSession(w)

	done := make(chan struct{})
	var bg sync.WaitGroup
	defer func() {
		conn.Close()
		close(done)
		bg.Wait()
		sess.Close()
	}()

	pumping := false
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) {
				s.log.Warn("protocol error", "remote", remote, "err", err)
				wmu.Lock()
				w.WriteError("ERR " + err.Error())
				w.Flush()
				wmu.Unlock()
			} else if !errors.Is(err, io.EOF) {
				s.log.Debug("read failed", "remote", remote, "err", err)
			}
			return
		}

		wmu.Lock()
		if sess.Blocks(args) {
			if err = w.Flush(); err == nil {
				runBlocking(ctx, conn, r, func(ctx context.Context) {
					sess.Dispatch(ctx, args)
				})
			}
		} else {
			sess.Dispatch(ctx, args)
		}
		if err == nil && r.Buffered() == 0 {
			err = w.Flush()
		}
		wmu.Unlock()
		if err != nil {
			return
		}

		if sub := sess.Subscriber(); sub != nil && !pumping {
			pumping = true
			bg.Go(func() { pump(w, &wmu, sub, done) })
			bg.Go(func() {
				select {
				case <-sub.Kicked():
					s.log.Warn("slow subscriber disconnected", "remote", remote)
					conn.Close()
				case <-done:
				}
			})
		}
	}
}

func pump(w *resp.Writer, wmu *sync.Mutex, sub *pubsub.Subscriber, done <-chan struct{}) {
	msgs := sub.Messages()
	for {
		select {
		case <-done:
			return
		case m := <-msgs:
			wmu.Lock()
			w.WriteArray(3)
			w.WriteBulk("message")
			w.WriteBulk(m.Channel)
			w.WriteBulk(m.Payload)
			var err error
			if len(msgs) == 0 {
				err = w.Flush()
			}
			wmu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

func runBlocking(ctx context.Context, conn net.Conn, r *resp.Reader, run func(context.Context)) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if r.Buffered() > 0 {
		run(ctx)
		return
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if r.WaitReadable() != nil {
			cancel()
		}
	}()

	run(ctx)

	conn.SetReadDeadline(time.Now())
	<-done
	conn.SetReadDeadline(time.Time{})
}
