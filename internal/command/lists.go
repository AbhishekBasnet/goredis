package command

import (
	"context"
	"fmt"
	"strings"

	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

// LPUSH key element [element ...]
func lpush(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	push(st, w, args, true)
}

// RPUSH key element [element ...]
func rpush(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	push(st, w, args, false)
}

func push(st *store.Store, w *resp.Writer, args []string, front bool) {
	n, err := st.Push(args[1], front, args[2:]...)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteInteger(int64(n))
}

// LPOP key [count]
func lpop(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	pop(st, w, args, true)
}

// RPOP key [count]
func rpop(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	pop(st, w, args, false)
}

// pop replies with one bulk string, or with an array when a count is
// given. A missing key is a null of the matching kind.
func pop(st *store.Store, w *resp.Writer, args []string, front bool) {
	if len(args) > 3 {
		w.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(args[0])))
		return
	}

	count, withCount := 1, len(args) == 3
	if withCount {
		n, err := parseInt(args[2])
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if n < 0 {
			w.WriteError("ERR value is out of range, must be positive")
			return
		}
		count = int(n)
	}

	values, exists, err := st.Pop(args[1], front, count)
	switch {
	case err != nil:
		writeStoreError(w, err)
	case !withCount && !exists:
		w.WriteNull()
	case !withCount:
		w.WriteBulk(values[0])
	case !exists:
		w.WriteNullArray()
	default:
		writeBulks(w, values)
	}
}

// LLEN key
func llen(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	n, err := st.LLen(args[1])
	if err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteInteger(int64(n))
}

// LRANGE key start stop
func lrange(_ context.Context, st *store.Store, w *resp.Writer, args []string) {
	start, err1 := parseInt(args[2])
	stop, err2 := parseInt(args[3])
	if err1 != nil || err2 != nil {
		w.WriteError(errNotInteger.Error())
		return
	}

	values, err := st.LRange(args[1], int(start), int(stop))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeBulks(w, values)
}

// BLPOP key [key ...] timeout
func blpop(ctx context.Context, st *store.Store, w *resp.Writer, args []string) {
	bpop(ctx, st, w, args, true)
}

// BRPOP key [key ...] timeout
func brpop(ctx context.Context, st *store.Store, w *resp.Writer, args []string) {
	bpop(ctx, st, w, args, false)
}

// bpop replies with [key, element], or a null array on timeout.
func bpop(ctx context.Context, st *store.Store, w *resp.Writer, args []string, front bool) {
	timeout, err := parseTimeout(args[len(args)-1])
	if err != nil {
		w.WriteError(err.Error())
		return
	}

	p, ok, err := st.BPop(ctx, args[1:len(args)-1], front, timeout)
	switch {
	case err != nil:
		writeStoreError(w, err)
	case !ok:
		w.WriteNullArray()
	default:
		writeBulks(w, []string{p.Key, p.Value})
	}
}

func writeBulks(w *resp.Writer, values []string) {
	w.WriteArray(len(values))
	for _, v := range values {
		w.WriteBulk(v)
	}
}
