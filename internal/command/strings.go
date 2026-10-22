package command

import (
	"strings"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

const errSyntax = "ERR syntax error"

// GET key
func get(st *store.Store, w *resp.Writer, args []string) {
	v, ok := st.Get(args[1])
	if !ok {
		w.WriteNull()
		return
	}
	w.WriteBulk(v)
}

// SET key value [EX seconds | PX milliseconds]
func set(st *store.Store, w *resp.Writer, args []string) {
	var expiry time.Duration
	for i := 3; i < len(args); i += 2 {
		var unit time.Duration
		switch strings.ToUpper(args[i]) {
		case "EX":
			unit = time.Second
		case "PX":
			unit = time.Millisecond
		}
		// Unknown option, EX and PX together, or an option with no value.
		if unit == 0 || expiry != 0 || i+1 == len(args) {
			w.WriteError(errSyntax)
			return
		}

		var err error
		if expiry, err = parseTTL(args[i+1], unit, "set"); err != nil {
			w.WriteError(err.Error())
			return
		}
		if expiry <= 0 {
			w.WriteError(invalidExpire("set"))
			return
		}
	}
	st.Set(args[1], args[2], expiry)
	w.WriteSimple("OK")
}

// INCR key
func incr(st *store.Store, w *resp.Writer, args []string) {
	incrBy(st, w, args[1], 1)
}

// DECR key
func decr(st *store.Store, w *resp.Writer, args []string) {
	incrBy(st, w, args[1], -1)
}

func incrBy(st *store.Store, w *resp.Writer, key string, delta int64) {
	n, err := st.IncrBy(key, delta)
	if err != nil {
		// The store's error texts are Redis's own messages.
		w.WriteError("ERR " + err.Error())
		return
	}
	w.WriteInteger(n)
}

// MGET key [key ...]
func mget(st *store.Store, w *resp.Writer, args []string) {
	values, found := st.MGet(args[1:]...)
	w.WriteArray(len(values))
	for i, v := range values {
		if found[i] {
			w.WriteBulk(v)
		} else {
			w.WriteNull()
		}
	}
}

// MSET key value [key value ...]
func mset(st *store.Store, w *resp.Writer, args []string) {
	// Name plus pairs is always odd; the arity rule can't express that.
	if len(args)%2 == 0 {
		w.WriteError("ERR wrong number of arguments for 'mset' command")
		return
	}
	st.MSet(args[1:]...)
	w.WriteSimple("OK")
}
