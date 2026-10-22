package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/AbhishekBasnet/goredis/internal/command"
	"github.com/AbhishekBasnet/goredis/internal/server"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

func main() {
	addr := flag.String("addr", ":6380", "listen address")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	cmds := command.NewRegistry(store.New())
	srv := server.New(*addr, log, cmds)

	if err := srv.Run(); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
