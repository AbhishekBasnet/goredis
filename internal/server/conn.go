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

	// Stage 8 derives this from the server's context, so that shutting down
	// also wakes clients blocked in BLPOP.
	ctx := context.Background()

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if errors.Is(err, resp.ErrProtocol) {
				// Like Redis: tell the client what was wrong, then hang up,
				// because we can no longer tell where the next command starts.
				s.log.Warn("protocol error", "remote", remote, "err", err)
				w.WriteError("ERR " + err.Error())
				w.Flush()
			} else if !errors.Is(err, io.EOF) {
				s.log.Debug("read failed", "remote", remote, "err", err)
			}
			return
		}

		if s.cmds.Blocks(args) {
			// Replies to earlier pipelined commands must not sit in the
			// buffer while this one waits, possibly for a long time.
			if err := w.Flush(); err != nil {
				return
			}
			runBlocking(ctx, conn, r, func(ctx context.Context) {
				s.cmds.Dispatch(ctx, w, args)
			})
		} else {
			s.cmds.Dispatch(ctx, w, args)
		}

		// Pipelining: if the client already sent more commands, answer those
		// too before flushing, so a batch of N commands costs one write
		// syscall instead of N.
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

// runBlocking runs a command that may wait, with a context that is
// cancelled if the client hangs up in the meantime. Without this, a client
// that disconnects during BLPOP would stay queued, and the next push would
// hand its element to a dead connection, losing it.
//
// Nothing reads the connection while a command runs, so a watcher goroutine
// waits for the next byte. An error or EOF means the client is gone. If
// bytes arrive instead, the client is alive and sending more commands; the
// watcher stops, and a hang-up after that goes unnoticed until the command
// returns. That costs nothing for redis-cli and client libraries, which send
// nothing while they wait for a blocking reply.
func runBlocking(ctx context.Context, conn net.Conn, r *resp.Reader, run func(context.Context)) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if r.Buffered() > 0 {
		// More commands are already here, so the client is alive, and
		// waiting for "the next byte" would return at once.
		run(ctx)
		return
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if r.WaitReadable() != nil {
			cancel() // harmless if the command has already returned
		}
	}()

	run(ctx)

	// Wake the watcher by making its read time out right now, wait until it
	// has exited, so only one goroutine ever uses r, then clear the deadline.
	conn.SetReadDeadline(time.Now())
	<-done
	conn.SetReadDeadline(time.Time{})
}
