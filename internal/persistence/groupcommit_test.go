package persistence

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func openWithSync(t *testing.T, policy Fsync, sync func(real func() error) error) (*AOF, string, *atomic.Int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := OpenAOF(path, policy, discard)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	real := a.syncFile
	a.syncFile = func() error {
		calls.Add(1)
		return sync(real)
	}
	t.Cleanup(func() { a.Close() })
	return a, path, &calls
}

func TestGroupCommit(t *testing.T) {
	const writers = 100
	a, path, syncs := openWithSync(t, FsyncAlways, func(real func() error) error {
		time.Sleep(20 * time.Millisecond)
		return real()
	})

	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() {
			a.Append("SET", fmt.Sprint("k", i), "v")
			if err := a.WaitFor(a.Seq()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	if n := syncs.Load(); n >= writers/5 {
		t.Errorf("%d fsyncs for %d writes: commits were not grouped", n, writers)
	}
	a.Close()
	if got, _ := collect(t, LoadAOF, path); len(got) != writers {
		t.Errorf("file holds %d commands, want %d", len(got), writers)
	}
}

func TestWaitForAlwaysWaitsForFsync(t *testing.T) {
	release := make(chan struct{})
	a, _, _ := openWithSync(t, FsyncAlways, func(real func() error) error {
		<-release
		return real()
	})

	a.Append("SET", "k", "v")
	done := make(chan error, 1)
	go func() { done <- a.WaitFor(a.Seq()) }()

	select {
	case <-done:
		t.Fatal("WaitFor returned before the fsync finished")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWaitForEverysecDoesNotWaitForFsync(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	a, path, _ := openWithSync(t, FsyncEverySec, func(real func() error) error {
		<-block
		return real()
	})

	a.Append("SET", "k", "v")
	start := time.Now()
	if err := a.WaitFor(a.Seq()); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("WaitFor took %v under everysec", d)
	}
	if got, _ := collect(t, LoadAOF, path); len(got) != 1 {
		t.Errorf("file holds %d commands, want 1", len(got))
	}
}

func TestEverysecSyncs(t *testing.T) {
	a, _, syncs := openWithSync(t, FsyncEverySec, func(real func() error) error { return real() })
	a.Append("SET", "k", "v")
	a.WaitFor(a.Seq())

	deadline := time.Now().Add(3 * time.Second)
	for syncs.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no fsync within 3 seconds under everysec")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestWaitForReturnsOnWriteFailure(t *testing.T) {
	a, _, _ := openWithSync(t, FsyncNo, func(real func() error) error { return real() })
	a.f.Close()

	a.Append("SET", "k", "v")
	done := make(chan error, 1)
	go func() { done <- a.WaitFor(a.Seq()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Error("WaitFor reported success for a failed write")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitFor hung after a write failure")
	}
}

func TestWaitForAfterClose(t *testing.T) {
	a, _, _ := openWithSync(t, FsyncAlways, func(real func() error) error { return real() })
	a.Append("SET", "k", "v")
	seq := a.Seq()
	a.Close()
	if err := a.WaitFor(seq); err != nil {
		t.Errorf("WaitFor for a write flushed by Close = %v", err)
	}
	if err := a.WaitFor(seq + 1); !errors.Is(err, errClosed) {
		t.Errorf("WaitFor for a write that never happened = %v, want errClosed", err)
	}
}
