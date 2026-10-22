package command

import (
	"context"
	"strings"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

const errSyntax = "ERR syntax error"

func get(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	v, ok, err := st.Get(args[1])
	switch {
	case err != nil:
		writeStoreError(w, err)
	case !ok:
		w.WriteNull()
	default:
		w.WriteBulk(v)
	}
}

func set(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	var expiry time.Duration
	for i := 3; i < len(args); i += 2 {
		var unit time.Duration
		switch strings.ToUpper(args[i]) {
		case "EX":
			unit = time.Second
		case "PX":
			unit = time.Millisecond
		}
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

func incr(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	incrBy(st, w, args[1], 1)
}

func decr(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	incrBy(st, w, args[1], -1)
}

func incrBy(st *store.Store, w *resp.Writer, key string, delta int64) {
	n, err := st.IncrBy(key, delta)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteInteger(n)
}

func mget(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
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

func mset(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	if len(args)%2 == 0 {
		w.WriteError("ERR wrong number of arguments for 'mset' command")
		return
	}
	st.MSet(args[1:]...)
	w.WriteSimple("OK")
}
