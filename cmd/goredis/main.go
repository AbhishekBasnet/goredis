package main

import (
	"context"
	"errors"
	"flag"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/AbhishekBasnet/goredis/internal/command"
	"github.com/AbhishekBasnet/goredis/internal/persistence"
	"github.com/AbhishekBasnet/goredis/internal/pubsub"
	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/server"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

func main() {
	addr := flag.String("addr", ":6380", "listen address")
	dir := flag.String("dir", ".", "directory for the AOF and snapshot files")
	appendonly := flag.Bool("appendonly", true, "log every write to the append-only file")
	appendfsync := flag.String("appendfsync", "everysec", "when to fsync the AOF: always, everysec or no")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	policy, err := persistence.ParseFsync(*appendfsync)
	if err != nil {
		log.Error("bad flag", "err", err)
		os.Exit(2)
	}
	aofPath := filepath.Join(*dir, "appendonly.aof")
	snapPath := filepath.Join(*dir, "dump.resp")

	st := store.New()
	fromSnapshot, err := loadData(log, st, aofPath, snapPath, *appendonly)
	if err != nil {
		log.Error("loading data failed", "err", err)
		os.Exit(1)
	}

	var aof *persistence.AOF
	if *appendonly {
		if aof, err = persistence.OpenAOF(aofPath, policy, log); err != nil {
			log.Error("opening AOF failed", "err", err)
			os.Exit(1)
		}
		if fromSnapshot {
			for _, cmd := range st.Dump() {
				aof.Append(cmd...)
			}
		}
		st.SetJournal(aof)
	}
	snaps := persistence.NewSnapshotter(snapPath, st, log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var bg sync.WaitGroup
	bg.Go(func() { st.RunExpiry(ctx, store.DefaultExpiryInterval) })
	if aof != nil {
		bg.Go(func() { aof.Run(ctx) })
	}

	cmds := command.NewRegistry(st, pubsub.NewBroker(pubsub.DefaultBufferSize), snaps)
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
	snaps.Wait()
	if aof != nil {
		if err := aof.Close(); err != nil {
			log.Error("closing AOF failed", "err", err)
			code = 1
		}
	}
	log.Info("background tasks stopped")
	os.Exit(code)
}

func loadData(log *slog.Logger, st *store.Store, aofPath, snapPath string, appendonly bool) (fromSnapshot bool, err error) {
	if appendonly {
		res, err := persistence.LoadAOF(aofPath, st.Apply)
		switch {
		case err == nil:
			if res.TruncatedBytes > 0 {
				log.Warn("AOF ended with an incomplete command; truncated it", "path", aofPath, "bytes", res.TruncatedBytes)
			}
			log.Info("loaded AOF", "path", aofPath, "commands", res.Commands)
			return false, nil
		case !errors.Is(err, fs.ErrNotExist):
			return false, err
		}
	}

	res, err := persistence.LoadSnapshot(snapPath, st.Apply)
	switch {
	case err == nil:
		log.Info("loaded snapshot", "path", snapPath, "commands", res.Commands)
		return true, nil
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	default:
		return false, err
	}
}
