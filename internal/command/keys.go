package command

import (
	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

// DEL key [key ...]
func del(st *store.Store, w *resp.Writer, args []string) {
	w.WriteInteger(int64(st.Del(args[1:]...)))
}

// EXISTS key [key ...]
func exists(st *store.Store, w *resp.Writer, args []string) {
	w.WriteInteger(int64(st.Exists(args[1:]...)))
}
