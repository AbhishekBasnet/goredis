package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestPushPop(t *testing.T) {
	s := New()

	if n, _ := s.Push("l", true, "a", "b", "c"); n != 3 {
		t.Errorf("LPUSH returned %d, want 3", n)
	}
	if n, _ := s.Push("l", false, "x", "y"); n != 5 {
		t.Errorf("RPUSH returned %d, want 5", n)
	}
	// LPUSH a b c pushes one at a time, so the list reads c b a.
	if got, _ := s.LRange("l", 0, -1); !slices.Equal(got, []string{"c", "b", "a", "x", "y"}) {
		t.Errorf("list = %q", got)
	}

	if got, _, _ := s.Pop("l", true, 2); !slices.Equal(got, []string{"c", "b"}) {
		t.Errorf("LPOP 2 = %q", got)
	}
	if got, _, _ := s.Pop("l", false, 1); !slices.Equal(got, []string{"y"}) {
		t.Errorf("RPOP = %q", got)
	}
	if got, _, _ := s.Pop("l", true, 10); !slices.Equal(got, []string{"a", "x"}) {
		t.Errorf("LPOP 10 = %q, want the 2 that are left", got)
	}

	// Popping the last element deletes the key.
	if n := s.Exists("l"); n != 0 {
		t.Error("empty list still exists")
	}
	if _, exists, _ := s.Pop("l", true, 1); exists {
		t.Error("Pop on a missing key reported it exists")
	}
	if n, _ := s.LLen("l"); n != 0 {
		t.Errorf("LLEN of missing key = %d", n)
	}
}

func TestLRange(t *testing.T) {
	s := New()
	s.Push("l", false, "a", "b", "c", "d", "e")

	tests := []struct {
		start, stop int
		want        []string
	}{
		{0, -1, []string{"a", "b", "c", "d", "e"}},
		{1, 2, []string{"b", "c"}},
		{-2, -1, []string{"d", "e"}},
		{-100, 1, []string{"a", "b"}},
		{3, 100, []string{"d", "e"}},
		{2, 1, nil},
		{5, 10, nil},
		{-1, -2, nil},
	}
	for _, tt := range tests {
		got, err := s.LRange("l", tt.start, tt.stop)
		if err != nil || !slices.Equal(got, tt.want) {
			t.Errorf("LRange(%d, %d) = %q, %v; want %q", tt.start, tt.stop, got, err, tt.want)
		}
	}
}

func TestWrongType(t *testing.T) {
	s := New()
	s.Set("str", "v", 0)
	s.Push("list", false, "a")

	checks := []struct {
		name string
		err  error
	}{
		{"Push on string", second(s.Push("str", true, "x"))},
		{"Pop on string", third(s.Pop("str", true, 1))},
		{"LLen on string", second(s.LLen("str"))},
		{"LRange on string", second(s.LRange("str", 0, -1))},
		{"Get on list", third(s.Get("list"))},
		{"IncrBy on list", second(s.IncrBy("list", 1))},
	}
	for _, c := range checks {
		if !errors.Is(c.err, ErrWrongType) {
			t.Errorf("%s: err = %v, want ErrWrongType", c.name, c.err)
		}
	}

	// MGET treats a list as missing rather than failing.
	if _, found := s.MGet("list"); found[0] {
		t.Error("MGet found a list")
	}
	// SET replaces a list without complaint.
	s.Set("list", "now a string", 0)
	if v, _, err := s.Get("list"); err != nil || v != "now a string" {
		t.Errorf("Get after SET = %q, %v", v, err)
	}
}

func second[A, B any](_ A, b B) B        { return b }
func third[A, B, C any](_ A, _ B, c C) C { return c }

func TestBPopImmediate(t *testing.T) {
	s := New()
	s.Push("b", false, "1", "2")

	// Keys are checked in order: "a" is missing, so "b" is served.
	p, ok, err := s.BPop(context.Background(), []string{"a", "b"}, true, time.Second)
	if err != nil || !ok || p != (Popped{"b", "1"}) {
		t.Errorf("BPop = %+v, %v, %v", p, ok, err)
	}

	s.Set("str", "v", 0)
	if _, _, err := s.BPop(context.Background(), []string{"str"}, true, time.Second); !errors.Is(err, ErrWrongType) {
		t.Errorf("BPop on a string: err = %v", err)
	}
}

func TestBPopTimeout(t *testing.T) {
	s := New()
	start := time.Now()
	_, ok, err := s.BPop(context.Background(), []string{"k"}, true, 50*time.Millisecond)
	if ok || err != nil {
		t.Fatalf("BPop = %v, %v; want a timeout", ok, err)
	}
	if d := time.Since(start); d < 50*time.Millisecond {
		t.Errorf("returned after %v, before the timeout", d)
	}
	if len(s.waiters) != 0 {
		t.Error("timed-out waiter is still queued")
	}
}

func TestBPopCancel(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if _, ok, _ := s.BPop(ctx, []string{"k"}, true, 0); ok {
		t.Fatal("cancelled BPop returned a value")
	}
	if len(s.waiters) != 0 {
		t.Error("cancelled waiter is still queued")
	}
	// The element must stay in the list for the next client, not vanish.
	s.Push("k", false, "v")
	if n, _ := s.LLen("k"); n != 1 {
		t.Errorf("LLEN = %d, want 1", n)
	}
}

// waitForWaiters blocks until n clients are queued on key.
func waitForWaiters(t *testing.T, s *Store, key string, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		got := len(s.waiters[key])
		s.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d waiters on %q, want %d", got, key, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// Blocked clients are served in the order they started waiting, and a
// client waiting on several keys is served by whichever is pushed first.
func TestBPopFIFOAndMultiKey(t *testing.T) {
	s := New()
	results := make(chan string, 3)
	block := func(name string, keys ...string) {
		go func() {
			p, _, _ := s.BPop(context.Background(), keys, true, 0)
			results <- name + "=" + p.Value
		}()
	}

	block("first", "q")
	waitForWaiters(t, s, "q", 1)
	block("second", "q")
	waitForWaiters(t, s, "q", 2)
	block("multi", "other", "q")
	waitForWaiters(t, s, "q", 3)

	n, _ := s.Push("q", false, "a", "b")
	if n != 2 {
		t.Errorf("RPUSH returned %d, want 2 (length before serving)", n)
	}
	// Who got what proves the order; the order the goroutines report back
	// in does not, so sort before comparing.
	served := []string{<-results, <-results}
	slices.Sort(served)
	if want := []string{"first=a", "second=b"}; !slices.Equal(served, want) {
		t.Errorf("served %q, want %q", served, want)
	}
	if n, _ := s.LLen("q"); n != 0 {
		t.Errorf("LLEN = %d: served elements were left in the list", n)
	}

	s.Push("other", false, "c")
	if got := <-results; got != "multi=c" {
		t.Errorf("got %s, want multi=c", got)
	}
	if len(s.waiters) != 0 {
		t.Errorf("waiters left: %v", s.waiters)
	}
}

// The plan's race test: many producers push, many consumers block-pop, and
// every element must arrive exactly once. Run it with -race.
func TestBPopProducersConsumers(t *testing.T) {
	const producers, consumers, perProducer = 10, 10, 500
	const total = producers * perProducer
	s := New()
	keys := []string{"q1", "q2", "q3"}

	got := make(chan string, total)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for range consumers {
		wg.Go(func() {
			for {
				p, ok, err := s.BPop(ctx, keys, true, 0)
				if err != nil {
					t.Error(err)
					return
				}
				if !ok {
					return // cancelled: the test is over
				}
				got <- p.Value
			}
		})
	}
	var producing sync.WaitGroup
	for i := range producers {
		producing.Go(func() {
			for j := range perProducer {
				s.Push(keys[j%len(keys)], j%2 == 0, fmt.Sprintf("%d-%d", i, j))
			}
		})
	}
	producing.Wait()

	seen := make(map[string]bool, total)
	for range total {
		select {
		case v := <-got:
			if seen[v] {
				t.Fatalf("%s delivered twice", v)
			}
			seen[v] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d elements delivered", len(seen), total)
		}
	}
	cancel()
	wg.Wait()

	if extra := len(got); extra != 0 {
		t.Errorf("%d extra deliveries", extra)
	}
	for _, k := range keys {
		if n, _ := s.LLen(k); n != 0 {
			t.Errorf("%s still holds %d elements", k, n)
		}
	}
}
