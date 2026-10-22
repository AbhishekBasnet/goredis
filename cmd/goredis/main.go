package main

import (
	"context"
	"errors"
	"flag"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/command"
	"github.com/AbhishekBasnet/goredis/internal/metrics"
	"github.com/AbhishekBasnet/goredis/internal/persistence"
	"github.com/AbhishekBasnet/goredis/internal/pubsub"
	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/server"
	"github.com/AbhishekBasnet/goredis/internal/stats"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

func main() {
	addr := flag.String("addr", ":6380", "listen address")
	dir := flag.String("dir", ".", "directory for the AOF and snapshot files")
	appendonly := flag.Bool("appendonly", true, "log every write to the append-only file")
	appendfsync := flag.String("appendfsync", "everysec", "when to fsync the AOF: always, everysec or no")
	maxClients := flag.Int("maxclients", 10000, "maximum connected clients, 0 for no limit")
	timeout := flag.Duration("timeout", 0, "close clients idle for this long, 0 to never")
	grace := flag.Duration("shutdown-grace", server.DefaultShutdownGrace, "how long shutdown waits for busy clients")
	metricsAddr := flag.String("metrics-addr", "", "serve Prometheus metrics on this address, e.g. :9121")
	flag.Parse()

	start := time.Now()
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
	broker := pubsub.NewBroker(pubsub.DefaultBufferSize)
	counters := &stats.Counters{}
	collect := stats.Sources{
		Counters:   counters,
		Start:      start,
		MaxClients: *maxClients,
		AOFEnabled: *appendonly,
		Keys:       st.KeyCount,
		Blocked:    st.BlockedClients,
		PubSub:     broker.Counts,
		Saving:     snaps.Saving,
	}.Collect

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var bg sync.WaitGroup
	bg.Go(func() { st.RunExpiry(ctx, store.DefaultExpiryInterval) })
	if aof != nil {
		bg.Go(func() { aof.Run(ctx) })
	}

	var metricsSrv *http.Server
	if *metricsAddr != "" {
		ln, err := net.Listen("tcp", *metricsAddr)
		if err != nil {
			log.Error("metrics listener failed", "err", err)
			os.Exit(1)
		}
		log.Info("metrics listening", "addr", ln.Addr().String())
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", metrics.Handler(collect))
		metricsSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		bg.Go(func() {
			if err := metricsSrv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
				log.Error("metrics server stopped", "err", err)
			}
		})
	}

	cmds := command.NewRegistry(command.Config{Store: st, Broker: broker, Saver: snaps, Counters: counters, Info: collect})
	srv := server.New(*addr, log, func(w *resp.Writer) server.Session { return cmds.NewClient(w) }, server.Config{
		MaxClients:    *maxClients,
		IdleTimeout:   *timeout,
		ShutdownGrace: *grace,
		Counters:      counters,
	})

	code := 0
	if err := srv.Run(ctx); err != nil {
		log.Error("server stopped", "err", err)
		code = 1
	}

	stop()
	if metricsSrv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), *grace)
		metricsSrv.Shutdown(shutdownCtx)
		cancel()
	}
	bg.Wait()
	snaps.Wait()
	if aof != nil {
		if err := aof.Close(); err != nil {
			log.Error("closing AOF failed", "err", err)
			code = 1
		}
	}
	log.Info("shutdown complete")
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
