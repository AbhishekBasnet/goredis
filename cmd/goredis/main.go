package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/AbhishekBasnet/goredis/internal/command"
	"github.com/AbhishekBasnet/goredis/internal/pubsub"
	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/server"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

func main() {
	addr := flag.String("addr", ":6380", "listen address")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st := store.New()
	var bg sync.WaitGroup
	bg.Go(func() { st.RunExpiry(ctx, store.DefaultExpiryInterval) })

	cmds := command.NewRegistry(st, pubsub.NewBroker(pubsub.DefaultBufferSize))
	srv := server.New(*addr, log, func(w *resp.Writer) server.Session { return cmds.NewClient(w) })
	errc := make(chan error, 1)
	go func() { errc <- srv.Run() }()

	code := 0
	select {
	case err := <-errc:
		log.Error("server stopped", "err", err)
		code = 1
	case <-ctx.Done():
		log.Info("shutting down")
	}

	stop()
	bg.Wait()
	log.Info("background tasks stopped")
	os.Exit(code)
}
