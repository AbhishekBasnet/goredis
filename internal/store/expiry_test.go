package store

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// newTestStore returns a store whose clock only moves when advance is called,
// so expiry tests are exact and never sleep.
func newTestStore() (s *Store, advance func(time.Duration)) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s = New()
	s.now = func() time.Time { return now }
	return s, func(d time.Duration) { now = now.Add(d) }
}

func TestSetWithTTL(t *testing.T) {
	s, advance := newTestStore()
	s.Set("k", "v", 10*time.Second)

	advance(9 * time.Second)
	if _, ok := s.Get("k"); !ok {
		t.Fatal("key expired early")
	}
	advance(time.Second)
	if _, ok := s.Get("k"); ok {
		t.Fatal("key still readable at its expiry time")
	}
	if n := s.Exists("k"); n != 0 {
		t.Errorf("Exists = %d after expiry", n)
	}
	if _, found := s.MGet("k"); found[0] {
		t.Error("MGet found an expired key")
	}
}

func TestSetClearsTTL(t *testing.T) {
	s, advance := newTestStore()
	s.Set("a", "1", time.Second)
	s.Set("a", "2", 0)
	s.Set("b", "1", time.Second)
	s.MSet("b", "2")

	advance(time.Hour)
	if v, ok := s.Get("a"); !ok || v != "2" {
		t.Error("SET without TTL did not clear the old TTL")
	}
	if v, ok := s.Get("b"); !ok || v != "2" {
		t.Error("MSET did not clear the old TTL")
	}
	if len(s.volatile) != 0 {
		t.Errorf("volatile index still has %d keys", len(s.volatile))
	}
}

func TestIncrKeepsTTL(t *testing.T) {
	s, advance := newTestStore()
	s.Set("n", "1", 10*time.Second)
	s.IncrBy("n", 1)

	if ttl, _ := s.TTL("n"); ttl != 10*time.Second {
		t.Errorf("TTL after INCR = %v, want 10s", ttl)
	}
	advance(10 * time.Second)
	// The old value is gone, so INCR starts again from 0, with no TTL.
	if n, _ := s.IncrBy("n", 1); n != 1 {
		t.Errorf("INCR on expired key = %d, want 1", n)
	}
	if ttl, _ := s.TTL("n"); ttl != NoExpiry {
		t.Errorf("TTL = %v, want NoExpiry", ttl)
	}
}

func TestExpireTTLPersist(t *testing.T) {
	s, advance := newTestStore()

	if s.Expire("missing", time.Second) {
		t.Error("Expire on a missing key returned true")
	}
	if _, exists := s.TTL("missing"); exists {
		t.Error("TTL on a missing key reported it exists")
	}

	s.Set("k", "v", 0)
	if ttl, _ := s.TTL("k"); ttl != NoExpiry {
		t.Errorf("TTL = %v, want NoExpiry", ttl)
	}
	if s.Persist("k") {
		t.Error("Persist on a key without TTL returned true")
	}

	s.Expire("k", 5*time.Second)
	advance(2 * time.Second)
	if ttl, _ := s.TTL("k"); ttl != 3*time.Second {
		t.Errorf("TTL = %v, want 3s", ttl)
	}

	if !s.Persist("k") {
		t.Error("Persist on a key with TTL returned false")
	}
	advance(time.Hour)
	if _, ok := s.Get("k"); !ok {
		t.Error("key expired after Persist")
	}
}

func TestExpireNonPositiveDeletes(t *testing.T) {
	s, _ := newTestStore()
	s.Set("k", "v", 0)
	if !s.Expire("k", 0) {
		t.Error("Expire(0) on an existing key returned false")
	}
	if _, ok := s.Get("k"); ok {
		t.Error("Expire(0) did not delete the key")
	}
}

func TestDelExpired(t *testing.T) {
	s, advance := newTestStore()
	s.Set("k", "v", time.Second)
	advance(time.Second)

	if n := s.Del("k"); n != 0 {
		t.Errorf("Del of an expired key = %d, want 0", n)
	}
	if len(s.data) != 0 || len(s.volatile) != 0 {
		t.Error("Del left the expired key in memory")
	}
}

// Reads never delete, but expired keys must not stay in memory forever:
// the active cycle removes them even if nobody touches them again.
func TestActiveExpire(t *testing.T) {
	s, advance := newTestStore()
	for i := range 1000 {
		s.Set("tmp"+strconv.Itoa(i), "v", time.Second)
	}
	for i := range 10 {
		s.Set("keep"+strconv.Itoa(i), "v", time.Hour)
		s.Set("forever"+strconv.Itoa(i), "v", 0)
	}
	advance(time.Second)

	// Each call runs up to 16 rounds of 20 samples, so a few ticks clear
	// the 1000 expired keys.
	for range 20 {
		s.activeExpire()
	}

	if got := len(s.data); got != 20 {
		t.Errorf("%d keys left in memory, want 20", got)
	}
	if got := len(s.volatile); got != 10 {
		t.Errorf("%d keys in the volatile index, want 10", got)
	}
}

func TestExpireSampleStopsAtLimit(t *testing.T) {
	s, advance := newTestStore()
	for i := range 100 {
		s.Set(strconv.Itoa(i), "v", time.Second)
	}
	advance(time.Second)

	sampled, expired := s.expireSample(20)
	if sampled != 20 || expired != 20 {
		t.Errorf("sampled %d, expired %d; want 20, 20", sampled, expired)
	}
	if len(s.data) != 80 {
		t.Errorf("%d keys left, want 80", len(s.data))
	}
}

// The background goroutine must actually run on its ticker, and must
// return when its context is cancelled; otherwise it leaks.
func TestRunExpiryLifecycle(t *testing.T) {
	s := New() // real clock: this test exercises the real ticker
	s.Set("k", "v", time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.RunExpiry(ctx, 5*time.Millisecond)
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		s.mu.RLock()
		n := len(s.data)
		s.mu.RUnlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background expiry never removed the key")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunExpiry did not return after cancel")
	}
}
