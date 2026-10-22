package persistence

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/resp"
)

type Fsync string

const (
	FsyncAlways   Fsync = "always"
	FsyncEverySec Fsync = "everysec"
	FsyncNo       Fsync = "no"
)

func ParseFsync(s string) (Fsync, error) {
	switch p := Fsync(s); p {
	case FsyncAlways, FsyncEverySec, FsyncNo:
		return p, nil
	}
	return "", fmt.Errorf("appendfsync must be always, everysec or no, not %q", s)
}

var errClosed = errors.New("AOF is closed")

type AOF struct {
	mu     sync.Mutex
	f      *os.File
	w      *resp.Writer
	policy Fsync
	dirty  bool
	err    error
	log    *slog.Logger
}

func OpenAOF(path string, policy Fsync, log *slog.Logger) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open AOF: %w", err)
	}
	return &AOF{f: f, w: resp.NewWriter(f), policy: policy, log: log}, nil
}

func (a *AOF) Append(args ...string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.err != nil {
		return
	}
	a.w.WriteArray(len(args))
	for _, arg := range args {
		a.w.WriteBulk(arg)
	}
	err := a.w.Flush()
	if err == nil && a.policy == FsyncAlways {
		err = a.f.Sync()
	}
	if err != nil {
		a.fail(err)
		return
	}
	a.dirty = true
}

func (a *AOF) Run(ctx context.Context) {
	if a.policy != FsyncEverySec {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.sync()
		}
	}
}

func (a *AOF) sync() {
	a.mu.Lock()
	needed := a.dirty && a.err == nil
	a.dirty = false
	a.mu.Unlock()

	if !needed {
		return
	}
	if err := a.f.Sync(); err != nil {
		a.mu.Lock()
		a.fail(err)
		a.mu.Unlock()
	}
}

func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if errors.Is(a.err, errClosed) {
		return nil
	}
	err := errors.Join(a.w.Flush(), a.f.Sync(), a.f.Close())
	a.err = errClosed
	return err
}

func (a *AOF) fail(err error) {
	a.err = err
	a.log.Error("AOF write failed; no further writes will be logged", "err", err)
}
