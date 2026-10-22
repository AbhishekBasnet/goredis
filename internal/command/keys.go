package command

import (
	"context"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

func del(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	w.WriteInteger(int64(st.Del(args[1:]...)))
}

func exists(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	w.WriteInteger(int64(st.Exists(args[1:]...)))
}

func expire(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	ttl, err := parseTTL(args[2], time.Second, "expire")
	if err != nil {
		w.WriteError(err.Error())
		return
	}
	w.WriteInteger(boolInt(st.Expire(args[1], ttl)))
}

func ttl(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	writeTTL(st, w, args[1], time.Second)
}

func pttl(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	writeTTL(st, w, args[1], time.Millisecond)
}

func writeTTL(st *store.Store, w *resp.Writer, key string, unit time.Duration) {
	d, ok := st.TTL(key)
	switch {
	case !ok:
		w.WriteInteger(-2)
	case d == store.NoExpiry:
		w.WriteInteger(-1)
	default:
		w.WriteInteger(int64(d.Round(unit) / unit))
	}
}

func persist(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	w.WriteInteger(boolInt(st.Persist(args[1])))
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
