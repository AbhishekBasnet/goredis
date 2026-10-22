package store

import (
	"errors"
	"math"
	"slices"
	"strconv"
	"sync"
	"testing"
)

func TestGetSet(t *testing.T) {
	s := New()
	if _, ok, _ := s.Get("k"); ok {
		t.Fatal("Get on empty store found a value")
	}
	s.Set("k", "v1", 0)
	s.Set("k", "v2", 0)
	if v, ok, _ := s.Get("k"); !ok || v != "v2" {
		t.Errorf("Get = %q, %v; want \"v2\", true", v, ok)
	}
}

func TestDelExists(t *testing.T) {
	s := New()
	s.MSet("a", "1", "b", "2")

	if n := s.Exists("a", "a", "b", "missing"); n != 3 {
		t.Errorf("Exists = %d, want 3 (duplicates count twice)", n)
	}
	if n := s.Del("a", "a", "missing"); n != 1 {
		t.Errorf("Del = %d, want 1", n)
	}
	if n := s.Exists("a"); n != 0 {
		t.Errorf("Exists after Del = %d, want 0", n)
	}
}

func TestMGetMSet(t *testing.T) {
	s := New()
	s.MSet("a", "1", "b", "2", "a", "3") // last write wins

	vals, found := s.MGet("a", "missing", "b")
	if want := []string{"3", "", "2"}; !slices.Equal(vals, want) {
		t.Errorf("values = %q, want %q", vals, want)
	}
	if want := []bool{true, false, true}; !slices.Equal(found, want) {
		t.Errorf("found = %v, want %v", found, want)
	}
}

func TestIncrBy(t *testing.T) {
	tests := []struct {
		name    string
		initial string // "" means the key is missing
		delta   int64
		want    int64
		wantErr error
	}{
		{"missing key starts at 0", "", 1, 1, nil},
		{"positive", "10", 5, 15, nil},
		{"negative result", "1", -3, -2, nil},
		{"max", strconv.FormatInt(math.MaxInt64-1, 10), 1, math.MaxInt64, nil},
		{"overflow", strconv.FormatInt(math.MaxInt64, 10), 1, 0, ErrOverflow},
		{"underflow", strconv.FormatInt(math.MinInt64, 10), -1, 0, ErrOverflow},
		{"not a number", "abc", 1, 0, ErrNotInteger},
		{"float", "1.5", 1, 0, ErrNotInteger},
		{"plus sign", "+1", 1, 0, ErrNotInteger},
		{"leading zero", "01", 1, 0, ErrNotInteger},
		{"space", " 1", 1, 0, ErrNotInteger},
		{"too big", "9223372036854775808", 1, 0, ErrNotInteger},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			if tt.initial != "" {
				s.Set("k", tt.initial, 0)
			}
			got, err := s.IncrBy("k", tt.delta)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
			// A failed increment must leave the old value untouched.
			if err != nil && tt.initial != "" {
				if v, _, _ := s.Get("k"); v != tt.initial {
					t.Errorf("value changed to %q after error", v)
				}
			}
		})
	}
}

// Many goroutines incrementing one key must not lose a single update.
// Without the lock in IncrBy this fails, and `go test -race` reports why.
func TestIncrByConcurrent(t *testing.T) {
	const workers, perWorker = 100, 1000
	s := New()

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range perWorker {
				if _, err := s.IncrBy("counter", 1); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()

	if v, _, _ := s.Get("counter"); v != strconv.Itoa(workers*perWorker) {
		t.Errorf("counter = %s, want %d", v, workers*perWorker)
	}
}
