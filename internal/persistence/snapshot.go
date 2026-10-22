package persistence

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/resp"
)

var ErrSaveInProgress = errors.New("Background save already in progress")

type Dumper interface {
	Dump() [][]string
}

type Snapshotter struct {
	path    string
	data    Dumper
	log     *slog.Logger
	running atomic.Bool
	wg      sync.WaitGroup
}

func NewSnapshotter(path string, data Dumper, log *slog.Logger) *Snapshotter {
	return &Snapshotter{path: path, data: data, log: log}
}

func (s *Snapshotter) Save() error {
	if !s.running.CompareAndSwap(false, true) {
		return ErrSaveInProgress
	}
	defer s.running.Store(false)

	return s.write(s.data.Dump())
}

func (s *Snapshotter) BackgroundSave() error {
	if !s.running.CompareAndSwap(false, true) {
		return ErrSaveInProgress
	}
	cmds := s.data.Dump()

	s.wg.Go(func() {
		defer s.running.Store(false)
		if err := s.write(cmds); err != nil {
			s.log.Error("background save failed", "err", err)
		}
	})
	return nil
}

func (s *Snapshotter) Saving() bool {
	return s.running.Load()
}

func (s *Snapshotter) Wait() {
	s.wg.Wait()
}

func (s *Snapshotter) write(cmds [][]string) error {
	start := time.Now()
	if err := WriteSnapshot(s.path, cmds); err != nil {
		return err
	}
	s.log.Info("snapshot saved", "path", s.path, "commands", len(cmds), "took", time.Since(start))
	return nil
}

func WriteSnapshot(path string, cmds [][]string) (err error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
		}
	}()

	w := resp.NewWriter(tmp)
	for _, cmd := range cmds {
		w.WriteArray(len(cmd))
		for _, arg := range cmd {
			w.WriteBulk(arg)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	return nil
}
