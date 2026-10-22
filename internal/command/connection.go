package command

import (
	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

// PING [message]
func ping(_ *store.Store, w *resp.Writer, args []string) {
	switch len(args) {
	case 1:
		w.WriteSimple("PONG")
	case 2:
		w.WriteBulk(args[1])
	default:
		w.WriteError("ERR wrong number of arguments for 'ping' command")
	}
}

// ECHO message
func echo(_ *store.Store, w *resp.Writer, args []string) {
	w.WriteBulk(args[1])
}
