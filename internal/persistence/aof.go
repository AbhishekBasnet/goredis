package persistence

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
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
	f        *os.File
	policy   Fsync
	log      *slog.Logger
	syncFile func() error

	seq atomic.Uint64

	mu      sync.Mutex
	cond    *sync.Cond
	pending []byte
	written uint64
	synced  uint64
	err     error
	closing bool

	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

func OpenAOF(path string, policy Fsync, log *slog.Logger) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open AOF: %w", err)
	}
	a := &AOF{
		f:        f,
		policy:   policy,
		log:      log,
		syncFile: f.Sync,
		wake:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	a.cond = sync.NewCond(&a.mu)
	go a.flusher()
	return a, nil
}

func (a *AOF) Append(args ...string) {
	a.mu.Lock()
	if a.err != nil || a.closing {
		a.mu.Unlock()
		return
	}
	a.pending = resp.AppendArray(a.pending, args...)
	a.seq.Add(1)
	a.mu.Unlock()

	select {
	case a.wake <- struct{}{}:
	default:
	}
}

func (a *AOF) Seq() uint64 {
	return a.seq.Load()
}

func (a *AOF) WaitFor(seq uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for a.err == nil && a.durable() < seq {
		a.cond.Wait()
	}
	if a.durable() >= seq {
		return nil
	}
	return a.err
}

func (a *AOF) durable() uint64 {
	if a.policy == FsyncAlways {
		return a.synced
	}
	return a.written
}

func (a *AOF) Close() error {
	a.mu.Lock()
	if a.closing {
		a.mu.Unlock()
		return nil
	}
	a.closing = true
	a.mu.Unlock()

	close(a.stop)
	<-a.done

	a.mu.Lock()
	defer a.mu.Unlock()
	err := a.err
	if cerr := a.f.Close(); err == nil {
		err = cerr
	}
	if a.err == nil {
		a.err = errClosed
	}
	a.cond.Broadcast()
	return err
}

func (a *AOF) flusher() {
	defer close(a.done)

	var tick <-chan time.Time
	if a.policy == FsyncEverySec {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		tick = ticker.C
	}

	var spare []byte
	for {
		select {
		case <-a.wake:
			spare = a.flush(spare)
		case <-tick:
			a.sync()
		case <-a.stop:
			a.flush(spare)
			a.sync()
			return
		}
	}
}

func (a *AOF) flush(spare []byte) []byte {
	a.mu.Lock()
	if len(a.pending) == 0 || a.err != nil {
		a.mu.Unlock()
		return spare
	}
	batch, upTo := a.pending, a.seq.Load()
	a.pending = spare[:0]
	a.mu.Unlock()

	_, err := a.f.Write(batch)
	if err == nil && a.policy == FsyncAlways {
		err = a.syncFile()
	}

	a.mu.Lock()
	if err != nil {
		a.fail(err)
	} else {
		a.written = upTo
		if a.policy == FsyncAlways {
			a.synced = upTo
		}
	}
	a.cond.Broadcast()
	a.mu.Unlock()
	return batch
}

func (a *AOF) sync() {
	a.mu.Lock()
	upTo, needed := a.written, a.written > a.synced && a.err == nil
	a.mu.Unlock()
	if !needed {
		return
	}

	err := a.syncFile()

	a.mu.Lock()
	if err != nil {
		a.fail(err)
	} else {
		a.synced = upTo
	}
	a.cond.Broadcast()
	a.mu.Unlock()
}

func (a *AOF) fail(err error) {
	a.err = err
	a.log.Error("AOF write failed; no further writes will be logged", "err", err)
}
