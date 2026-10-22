package command

import (
	"context"
	"io"
	"testing"

	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

func benchmarkDispatch(b *testing.B, args ...string) {
	c := NewRegistry(Config{Store: store.New()}).NewClient(resp.NewWriter(io.Discard))
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		c.Dispatch(ctx, args)
	}
}

func BenchmarkDispatchSet(b *testing.B)  { benchmarkDispatch(b, "SET", "key", "xxx") }
func BenchmarkDispatchGet(b *testing.B)  { benchmarkDispatch(b, "GET", "key") }
func BenchmarkDispatchIncr(b *testing.B) { benchmarkDispatch(b, "INCR", "counter") }
func BenchmarkDispatchPing(b *testing.B) { benchmarkDispatch(b, "PING") }
