// Package command maps command names to handlers. Handlers read and write
// the store and encode their reply with a resp.Writer.
package command

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

// Handler runs one command. args[0] is the command name as the client typed
// it; the registry has already checked the argument count.
//
// ctx is cancelled if the client goes away while a blocking command waits.
// Handlers that never wait ignore it.
type Handler func(ctx context.Context, st *store.Store, w *resp.Writer, args []string)

// Command describes one entry in the command table.
type Command struct {
	Name string // lower case, as Redis prints it in error messages

	// Arity counts the command name too, following Redis's convention:
	// a positive number means exactly that many arguments, a negative
	// number means at least -Arity. GET is 2, DEL is -2.
	Arity int

	Handler Handler

	// Blocking commands can wait for another client (BLPOP). The server
	// flushes earlier replies before running them and watches for the
	// client hanging up while they wait.
	Blocking bool
}

// Registry is the command table. It is built once at startup and only read
// afterwards, so it needs no lock.
type Registry struct {
	store *store.Store
	cmds  map[string]Command
}

func NewRegistry(st *store.Store) *Registry {
	r := &Registry{store: st, cmds: make(map[string]Command)}
	for _, c := range []Command{
		// connection.go
		{Name: "ping", Arity: -1, Handler: ping},
		{Name: "echo", Arity: 2, Handler: echo},
		// strings.go
		{Name: "get", Arity: 2, Handler: get},
		{Name: "set", Arity: -3, Handler: set},
		{Name: "incr", Arity: 2, Handler: incr},
		{Name: "decr", Arity: 2, Handler: decr},
		{Name: "mget", Arity: -2, Handler: mget},
		{Name: "mset", Arity: -3, Handler: mset},
		// keys.go
		{Name: "del", Arity: -2, Handler: del},
		{Name: "exists", Arity: -2, Handler: exists},
		{Name: "expire", Arity: 3, Handler: expire},
		{Name: "ttl", Arity: 2, Handler: ttl},
		{Name: "pttl", Arity: 2, Handler: pttl},
		{Name: "persist", Arity: 2, Handler: persist},
		// lists.go
		{Name: "lpush", Arity: -3, Handler: lpush},
		{Name: "rpush", Arity: -3, Handler: rpush},
		{Name: "lpop", Arity: -2, Handler: lpop},
		{Name: "rpop", Arity: -2, Handler: rpop},
		{Name: "llen", Arity: 2, Handler: llen},
		{Name: "lrange", Arity: 4, Handler: lrange},
		{Name: "blpop", Arity: -3, Handler: blpop, Blocking: true},
		{Name: "brpop", Arity: -3, Handler: brpop, Blocking: true},
	} {
		r.cmds[c.Name] = c
	}
	return r
}

// Dispatch looks up args[0] case-insensitively, checks the argument count
// and runs the handler. Every outcome, including errors, is written to w.
func (r *Registry) Dispatch(ctx context.Context, w *resp.Writer, args []string) {
	cmd, ok := r.cmds[strings.ToLower(args[0])]
	if !ok {
		w.WriteError(unknownCommand(args))
		return
	}
	if !arityOK(cmd.Arity, len(args)) {
		w.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd.Name))
		return
	}
	cmd.Handler(ctx, r.store, w, args)
}

// Blocks reports whether args is a command that may wait for another client.
func (r *Registry) Blocks(args []string) bool {
	return r.cmds[strings.ToLower(args[0])].Blocking
}

func arityOK(arity, n int) bool {
	if arity >= 0 {
		return n == arity
	}
	return n >= -arity
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

// writeStoreError replies with a store error. The store's texts are Redis's
// own messages without the error code, which this adds: WRONGTYPE for type
// mismatches, ERR for everything else.
func writeStoreError(w *resp.Writer, err error) {
	code := "ERR "
	if errors.Is(err, store.ErrWrongType) {
		code = "WRONGTYPE "
	}
	w.WriteError(code + err.Error())
}
