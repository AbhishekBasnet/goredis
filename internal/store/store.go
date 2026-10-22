// Package store holds the in-memory dataset. It knows nothing about RESP or
// networking; every method is safe to call from many goroutines at once.
package store

import (
	"errors"
	"math"
	"strconv"
	"sync"
)

var (
	ErrNotInteger = errors.New("value is not an integer or out of range")
	ErrOverflow   = errors.New("increment or decrement would overflow")
)

// Store is a string → string map guarded by one RWMutex. Readers (GET, MGET,
// EXISTS) share the lock; writers take it exclusively.
//
// Every method takes the lock once and does all its work inside, so a
// read-modify-write such as IncrBy can never interleave with another client.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func New() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.data[key]
	return v, ok
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

// MGet looks up all keys under one lock, so the result is a consistent
// snapshot. found[i] reports whether keys[i] exists.
func (s *Store) MGet(keys ...string) (values []string, found []bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	values = make([]string, len(keys))
	found = make([]bool, len(keys))
	for i, k := range keys {
		values[i], found[i] = s.data[k]
	}
	return values, found
}

// MSet sets key/value pairs atomically: no reader sees half of them.
// pairs is key1, value1, key2, value2, ...; the caller ensures an even length.
func (s *Store) MSet(pairs ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i+1 < len(pairs); i += 2 {
		s.data[pairs[i]] = pairs[i+1]
	}
}

// Del removes keys and returns how many existed.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0
	for _, k := range keys {
		if _, ok := s.data[k]; ok {
			delete(s.data, k)
			n++
		}
	}
	return n
}

// Exists returns how many of keys exist. Like Redis, a key named twice is
// counted twice.
func (s *Store) Exists(keys ...string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n := 0
	for _, k := range keys {
		if _, ok := s.data[k]; ok {
			n++
		}
	}
	return n
}

// IncrBy adds delta to the integer stored at key and returns the new value.
// A missing key counts as 0.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var n int64
	if v, ok := s.data[key]; ok {
		var err error
		if n, err = parseInt(v); err != nil {
			return 0, err
		}
	}

	if (delta > 0 && n > math.MaxInt64-delta) || (delta < 0 && n < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	n += delta
	s.data[key] = strconv.FormatInt(n, 10)
	return n, nil
}

// parseInt accepts only the canonical form of a 64-bit integer, as Redis
// does: "10" and "-3" are fine, but "+1", "01", " 1" and "1.0" are not.
// strconv.ParseInt alone would accept the first two.
func parseInt(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != s {
		return 0, ErrNotInteger
	}
	return n, nil
}
