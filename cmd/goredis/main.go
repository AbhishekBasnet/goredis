package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/AbhishekBasnet/goredis/internal/server"
)

func main() {
	addr := flag.String("addr", ":6380", "listen address")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	srv := server.New(*addr, log)

	if err := srv.Run(); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
