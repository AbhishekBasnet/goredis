package server

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/resp"
)

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	s.log.Info("Client connected", "remote", remote)
	defer s.log.Info("Client disconnected", "remote", remote)

	ctx := context.Background()

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) {
				s.log.Warn("protocol error", "remote", remote, "err", err)
				w.WriteError("ERR " + err.Error())
				w.Flush()
			} else if !errors.Is(err, io.EOF) {
				s.log.Debug("read failed", "remote", remote, "err", err)
			}
			return
		}

		if s.cmds.Blocks(args) {
			if err := w.Flush(); err != nil {
				return
			}
			runBlocking(ctx, conn, r, func(ctx context.Context) {
				s.cmds.Dispatch(ctx, w, args)
			})
		} else {
			s.cmds.Dispatch(ctx, w, args)
		}

		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
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
