package server

import (
	"errors"
	"io"
	"net"

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

		s.cmds.Dispatch(w, args)

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
