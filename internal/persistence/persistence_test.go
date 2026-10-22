package persistence

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/resp"
)

var discard = slog.New(slog.DiscardHandler)

func collect(t *testing.T, load func(string, func([]string) error) (LoadResult, error), path string) ([][]string, LoadResult) {
	t.Helper()
	var got [][]string
	res, err := load(path, func(args []string) error {
		got = append(got, args)
		return nil
	})
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return got, res
}

func equalCmds(a, b [][]string) bool {
	return slices.EqualFunc(a, b, slices.Equal[[]string])
}

func writeAOF(t *testing.T, path string, policy Fsync, cmds ...[]string) {
	t.Helper()
	a, err := OpenAOF(path, policy, discard)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cmds {
		a.Append(c...)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAOFRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	first := [][]string{{"SET", "k", "v"}, {"RPUSH", "l", "a", "b\r\nc", ""}}
	second := [][]string{{"DEL", "k"}}

	for _, policy := range []Fsync{FsyncAlways, FsyncEverySec, FsyncNo} {
		os.Remove(path)
		writeAOF(t, path, policy, first...)
		writeAOF(t, path, policy, second...)

		got, res := collect(t, LoadAOF, path)
		if want := append(slices.Clone(first), second...); !equalCmds(got, want) {
			t.Errorf("%s: loaded %q, want %q", policy, got, want)
		}
		if res.TruncatedBytes != 0 {
			t.Errorf("%s: truncated %d bytes of a clean file", policy, res.TruncatedBytes)
		}
	}
}

func TestWaitForMeansWritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := OpenAOF(path, FsyncNo, discard)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	a.Append("SET", "k", "v")
	if err := a.WaitFor(a.Seq()); err != nil {
		t.Fatal(err)
	}
	if got, _ := collect(t, LoadAOF, path); len(got) != 1 {
		t.Errorf("after WaitFor the file holds %d commands, want 1", len(got))
	}
}

func TestLoadRepairsEveryTruncationPoint(t *testing.T) {
	dir := t.TempDir()
	full := filepath.Join(dir, "full.aof")
	cmds := [][]string{{"SET", "a", "1"}, {"RPUSH", "list", "x", "y"}, {"DEL", "a"}}
	writeAOF(t, full, FsyncNo, cmds...)
	data, err := os.ReadFile(full)
	if err != nil {
		t.Fatal(err)
	}

	for cut := range len(data) + 1 {
		path := filepath.Join(dir, "cut.aof")
		if err := os.WriteFile(path, data[:cut], 0o644); err != nil {
			t.Fatal(err)
		}

		got, res := collect(t, LoadAOF, path)
		if !equalCmds(got, cmds[:len(got)]) {
			t.Fatalf("cut at %d: loaded %q, not a prefix of the log", cut, got)
		}
		info, _ := os.Stat(path)
		if info.Size()+res.TruncatedBytes != int64(cut) {
			t.Fatalf("cut at %d: size %d + truncated %d != %d", cut, info.Size(), res.TruncatedBytes, cut)
		}

		writeAOF(t, path, FsyncNo, []string{"PING"})
		after, res := collect(t, LoadAOF, path)
		if want := append(slices.Clone(got), []string{"PING"}); !equalCmds(after, want) || res.TruncatedBytes != 0 {
			t.Fatalf("cut at %d: after repair and append, loaded %q", cut, after)
		}
	}
}

func TestLoadRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	if err := os.WriteFile(path, []byte("*1\r\n$4\r\nPING\r\n*x\r\n*1\r\n$4\r\nPING\r\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadAOF(path, func([]string) error { return nil })
	if !errors.Is(err, resp.ErrProtocol) {
		t.Errorf("err = %v, want a protocol error", err)
	}
}

func TestLoadStopsOnApplyError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	writeAOF(t, path, FsyncNo, []string{"SET", "a", "1"}, []string{"BAD"}, []string{"SET", "b", "2"})

	boom := errors.New("boom")
	applied := 0
	res, err := LoadAOF(path, func(args []string) error {
		if args[0] == "BAD" {
			return boom
		}
		applied++
		return nil
	})
	if !errors.Is(err, boom) || applied != 1 || res.Commands != 1 {
		t.Errorf("err = %v, applied = %d, res = %+v", err, applied, res)
	}
}

func TestSnapshotIsNotRepaired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.resp")
	content := []byte("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPI")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadSnapshot(path, func([]string) error { return nil }); err == nil {
		t.Error("a truncated snapshot loaded without error")
	}
	if data, _ := os.ReadFile(path); len(data) != len(content) {
		t.Error("loading a snapshot modified the file")
	}
}

func TestParseFsync(t *testing.T) {
	for _, s := range []string{"always", "everysec", "no"} {
		if p, err := ParseFsync(s); err != nil || string(p) != s {
			t.Errorf("ParseFsync(%q) = %q, %v", s, p, err)
		}
	}
	if _, err := ParseFsync("sometimes"); err == nil {
		t.Error("ParseFsync accepted an unknown policy")
	}
}

func TestCloseStopsTheFlusher(t *testing.T) {
	for _, policy := range []Fsync{FsyncAlways, FsyncEverySec, FsyncNo} {
		a, err := OpenAOF(filepath.Join(t.TempDir(), "appendonly.aof"), policy, discard)
		if err != nil {
			t.Fatal(err)
		}
		a.Append("SET", "k", "v")
		a.Close()
		select {
		case <-a.done:
		case <-time.After(time.Second):
			t.Fatalf("%s: flusher still running after Close", policy)
		}
	}
}

func TestAppendAfterClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := OpenAOF(path, FsyncAlways, discard)
	if err != nil {
		t.Fatal(err)
	}
	a.Append("SET", "a", "1")
	a.Close()
	a.Append("SET", "b", "2")
	if err := a.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if got, _ := collect(t, LoadAOF, path); len(got) != 1 {
		t.Errorf("file holds %d commands, want 1", len(got))
	}
}

type fixedDump [][]string

func (d fixedDump) Dump() [][]string { return d }

func TestWriteSnapshotReplacesAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.resp")
	old := [][]string{{"SET", "old", "1"}}
	cur := [][]string{{"SET", "a", "1"}, {"RPUSH", "l", "x"}}

	if err := WriteSnapshot(path, old); err != nil {
		t.Fatal(err)
	}
	if err := WriteSnapshot(path, cur); err != nil {
		t.Fatal(err)
	}
	if got, _ := collect(t, LoadSnapshot, path); !equalCmds(got, cur) {
		t.Errorf("loaded %q, want %q", got, cur)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("directory holds %d files, want only the snapshot", len(entries))
	}

	if err := WriteSnapshot(filepath.Join(dir, "missing", "dump.resp"), cur); err == nil {
		t.Error("writing into a missing directory succeeded")
	}
	if got, _ := collect(t, LoadSnapshot, path); !equalCmds(got, cur) {
		t.Error("a failed write changed the existing snapshot")
	}
}

func TestSnapshotter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.resp")
	data := fixedDump{{"SET", "k", "v"}}
	s := NewSnapshotter(path, data, discard)

	if err := s.BackgroundSave(); err != nil {
		t.Fatal(err)
	}
	s.Wait()
	if got, _ := collect(t, LoadSnapshot, path); !equalCmds(got, data) {
		t.Errorf("BGSAVE wrote %q", got)
	}

	s.running.Store(true)
	if err := s.Save(); !errors.Is(err, ErrSaveInProgress) {
		t.Errorf("Save during a save = %v", err)
	}
	if err := s.BackgroundSave(); !errors.Is(err, ErrSaveInProgress) {
		t.Errorf("BackgroundSave during a save = %v", err)
	}
	s.running.Store(false)
	if err := s.Save(); err != nil {
		t.Errorf("Save = %v", err)
	}
}
