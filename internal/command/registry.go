// Package command maps command names to handlers. Handlers read and write
// the store and encode their reply with a resp.Writer.
package command

import (
	"fmt"
	"strings"

	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

// Handler runs one command. args[0] is the command name as the client typed
// it; the registry has already checked the argument count.
type Handler func(st *store.Store, w *resp.Writer, args []string)

// Command describes one entry in the command table.
type Command struct {
	Name string // lower case, as Redis prints it in error messages

	// Arity counts the command name too, following Redis's convention:
	// a positive number means exactly that many arguments, a negative
	// number means at least -Arity. GET is 2, DEL is -2.
	Arity int

	Handler Handler
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
		{"ping", -1, ping},
		{"echo", 2, echo},
		// strings.go
		{"get", 2, get},
		{"set", -3, set},
		{"incr", 2, incr},
		{"decr", 2, decr},
		{"mget", -2, mget},
		{"mset", -3, mset},
		// keys.go
		{"del", -2, del},
		{"exists", -2, exists},
	} {
		r.cmds[c.Name] = c
	}
	return r
}

// Dispatch looks up args[0] case-insensitively, checks the argument count
// and runs the handler. Every outcome, including errors, is written to w.
func (r *Registry) Dispatch(w *resp.Writer, args []string) {
	cmd, ok := r.cmds[strings.ToLower(args[0])]
	if !ok {
		w.WriteError(unknownCommand(args))
		return
	}
	if !arityOK(cmd.Arity, len(args)) {
		w.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd.Name))
		return
	}
	cmd.Handler(r.store, w, args)
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
