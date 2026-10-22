package server

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/AbhishekBasnet/goredis/internal/resp"
)

func (s *Server) handle(conn net.Conn) {
	defer conn.Close()

	remote := conn.RemoteAddr().String()
	s.log.Info("Client connected", "remote", remote)
	defer s.log.Info("Client disconnected", "remote", remote)

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

		dispatch(w, args)

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

// dispatch runs one command. It is a stand-in until Stage 3 adds the
// command registry.
func dispatch(w *resp.Writer, args []string) {
	switch strings.ToUpper(args[0]) {
	case "PING":
		switch len(args) {
		case 1:
			w.WriteSimple("PONG")
		case 2:
			w.WriteBulk(args[1])
		default:
			w.WriteError("ERR wrong number of arguments for 'ping' command")
		}
	default:
		w.WriteError(unknownCommand(args))
	}
}

// unknownCommand builds the same message Redis sends, e.g.
// ERR unknown command 'FOO', with args beginning with: 'a' 'b'
func unknownCommand(args []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ERR unknown command '%s', with args beginning with: ", args[0])
	for _, a := range args[1:] {
		fmt.Fprintf(&b, "'%s' ", a)
	}
	return b.String()
}
