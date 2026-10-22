package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
	"time"
)

type recorder struct {
	entries [][]string
}

func (r *recorder) Append(args ...string) {
	r.entries = append(r.entries, slices.Clone(args))
}

func replay(t *testing.T, src *Store, entries [][]string) *Store {
	t.Helper()
	dst := New()
	dst.now = src.now
	for _, e := range entries {
		if err := dst.Apply(e); err != nil {
			t.Fatalf("Apply(%q): %v", e, err)
		}
	}
	return dst
}

func equalDumps(a, b [][]string) bool {
	return slices.EqualFunc(a, b, slices.Equal[[]string])
}

func TestJournalReplayMatchesRandomOps(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	for seed := range uint64(20) {
		rng := rand.New(rand.NewPCG(seed, 99))
		s, advance := newTestStore()
		rec := &recorder{}
		s.SetJournal(rec)

		key := func() string { return keys[rng.IntN(len(keys))] }
		ttl := func() time.Duration { return time.Duration(rng.IntN(6)-1) * time.Second }
		value := func() string {
			if rng.IntN(2) == 0 {
				return strconv.Itoa(rng.IntN(100))
			}
			return fmt.Sprintf("v%d", rng.IntN(100))
		}

		for range 2000 {
			switch rng.IntN(11) {
			case 0:
				s.Set(key(), value(), max(ttl(), 0))
			case 1:
				s.MSet(key(), value(), key(), value())
			case 2:
				s.Del(key(), key())
			case 3:
				s.IncrBy(key(), int64(rng.IntN(7)-3))
			case 4:
				s.Expire(key(), ttl())
			case 5:
				s.Persist(key())
			case 6:
				s.Push(key(), rng.IntN(2) == 0, value(), value())
			case 7:
				s.Pop(key(), rng.IntN(2) == 0, rng.IntN(4))
			case 8:
				s.BPop(cancelled, []string{key(), key()}, rng.IntN(2) == 0, 0)
			case 9:
				advance(time.Duration(rng.IntN(1500)) * time.Millisecond)
			case 10:
				s.activeExpire()
			}
		}

		want := s.Dump()
		got := replay(t, s, rec.entries).Dump()
		if !equalDumps(got, want) {
			t.Fatalf("seed %d: replay differs\n got: %q\nwant: %q", seed, got, want)
		}
	}
}

func TestJournalServedWaiter(t *testing.T) {
	s, _ := newTestStore()
	rec := &recorder{}
	s.SetJournal(rec)

	served := make(chan Popped)
	go func() {
		p, _, _ := s.BPop(context.Background(), []string{"q"}, true, 0)
		served <- p
	}()
	waitForWaiters(t, s, "q", 1)
	s.Push("q", false, "a", "b")
	<-served

	want := [][]string{{"RPUSH", "q", "a", "b"}, {"LPOP", "q"}}
	if !equalDumps(rec.entries, want) {
		t.Fatalf("journal = %q, want %q", rec.entries, want)
	}
	if got := replay(t, s, rec.entries).Dump(); !equalDumps(got, [][]string{{"RPUSH", "q", "b"}}) {
		t.Errorf("replayed dataset = %q", got)
	}
}

func TestJournalKeepsAbsoluteExpiry(t *testing.T) {
	s, advance := newTestStore()
	rec := &recorder{}
	s.SetJournal(rec)

	s.Set("k", "v", time.Second)
	s.Expire("k", 100*time.Second)
	advance(2 * time.Second)

	r := replay(t, s, rec.entries)
	if d, ok := r.TTL("k"); !ok || d != 98*time.Second {
		t.Errorf("replayed TTL = %v, %v; want 98s", d, ok)
	}
}

func TestJournalPushOverExpiredKey(t *testing.T) {
	s, advance := newTestStore()
	rec := &recorder{}
	s.SetJournal(rec)

	s.Push("k", false, "old")
	s.Expire("k", time.Second)
	advance(2 * time.Second)
	s.Push("k", false, "new")

	r := replay(t, s, rec.entries)
	if got, _ := r.LRange("k", 0, -1); !slices.Equal(got, []string{"new"}) {
		t.Errorf("replayed list = %q, want [new]", got)
	}
	if d, _ := r.TTL("k"); d != NoExpiry {
		t.Errorf("replayed TTL = %v, want none", d)
	}
}

func TestFailedCommandsAreNotRecorded(t *testing.T) {
	s, _ := newTestStore()
	rec := &recorder{}
	s.Set("s", "text", 0)
	s.Push("l", false, "x")
	s.SetJournal(rec)

	s.IncrBy("s", 1)
	s.Push("s", true, "x")
	s.IncrBy("l", 1)
	s.Del("missing")
	s.Expire("missing", time.Second)
	s.Persist("s")
	s.Pop("missing", true, 1)
	s.Pop("l", true, 0)

	if len(rec.entries) != 0 {
		t.Errorf("recorded %q", rec.entries)
	}
}

func TestDump(t *testing.T) {
	s, _ := newTestStore()
	s.Set("b", "1", 0)
	s.Set("a", "2", time.Second)
	s.Push("c", false, "x", "y")
	s.Expire("c", time.Minute)
	s.Set("gone", "x", time.Millisecond)

	base := s.now().UnixMilli()
	want := [][]string{
		{"SET", "a", "2", "PXAT", strconv.FormatInt(base+1000, 10)},
		{"SET", "b", "1"},
		{"RPUSH", "c", "x", "y"},
		{"PEXPIREAT", "c", strconv.FormatInt(base+60_000, 10)},
		{"SET", "gone", "x", "PXAT", strconv.FormatInt(base+1, 10)},
	}
	if got := s.Dump(); !equalDumps(got, want) {
		t.Errorf("Dump =\n%q\nwant\n%q", got, want)
	}
}

func TestApplyRejectsBadEntries(t *testing.T) {
	s := New()
	s.Set("str", "v", 0)
	for _, args := range [][]string{
		{"SET"},
		{"SET", "k"},
		{"SET", "k", "v", "PXAT"},
		{"SET", "k", "v", "EX", "10"},
		{"SET", "k", "v", "PXAT", "soon"},
		{"MSET", "a", "1", "b"},
		{"PEXPIREAT", "k"},
		{"RPUSH", "str", "x"},
		{"LPOP", "k", "-1"},
		{"FLUSHALL", "x"},
	} {
		if err := s.Apply(args); !errors.Is(err, ErrBadEntry) {
			t.Errorf("Apply(%q) = %v, want ErrBadEntry", args, err)
		}
	}
}
