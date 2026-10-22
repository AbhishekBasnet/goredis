package command

import (
	"context"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

// DEL key [key ...]
func del(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	w.WriteInteger(int64(st.Del(args[1:]...)))
}

// EXISTS key [key ...]
func exists(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	w.WriteInteger(int64(st.Exists(args[1:]...)))
}

// EXPIRE key seconds
//
// Replies 1 if the key exists, 0 if not. A zero or negative TTL deletes the key.
func expire(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	ttl, err := parseTTL(args[2], time.Second, "expire")
	if err != nil {
		w.WriteError(err.Error())
		return
	}
	w.WriteInteger(boolInt(st.Expire(args[1], ttl)))
}

// TTL key: seconds left, rounded to the nearest second.
func ttl(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	writeTTL(st, w, args[1], time.Second)
}

// PTTL key: milliseconds left.
func pttl(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	writeTTL(st, w, args[1], time.Millisecond)
}

// writeTTL replies -2 for a missing key, -1 for a key with no expiry, or the
// time left in unit, rounded to the nearest unit as Redis does.
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

// PERSIST key: replies 1 if a TTL was removed, 0 otherwise.
func persist(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	w.WriteInteger(boolInt(st.Persist(args[1])))
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
