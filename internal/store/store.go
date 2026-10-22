// Package store holds the in-memory dataset. It knows nothing about RESP or
// networking; every method is safe to call from many goroutines at once.
package store

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"time"
)

var (
	ErrNotInteger = errors.New("value is not an integer or out of range")
	ErrOverflow   = errors.New("increment or decrement would overflow")
	ErrWrongType  = errors.New("Operation against a key holding the wrong kind of value")
)

// NoExpiry is what TTL returns for a key that exists but never expires.
const NoExpiry time.Duration = -1

type entry struct {
	value     any       // string or *list
	expiresAt time.Time // zero means the key never expires
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

// Store is a string → entry map guarded by one RWMutex. Readers (GET, MGET,
// EXISTS, TTL) share the lock; writers take it exclusively.
//
// Every method takes the lock once and does all its work inside, so a
// read-modify-write such as IncrBy can never interleave with another client.
//
// Expired keys are removed in two ways, like Redis:
//   - lazily: every command treats an expired key as missing. Read methods
//     hold only the read lock and so cannot delete it; write methods do.
//   - actively: a background goroutine samples keys with a TTL and deletes
//     the expired ones (see expiry.go), so keys nobody touches again do
//     not stay in memory forever.
type Store struct {
	mu   sync.RWMutex
	data map[string]entry

	// volatile holds the keys that have a TTL, so active expiry samples only
	// keys that can expire instead of the whole dataset.
	volatile map[string]struct{}

	// waiters holds the clients blocked in BLPOP/BRPOP on each key, oldest
	// first (see blocking.go).
	waiters map[string][]*waiter

	now func() time.Time // time.Now, replaced by a fake clock in tests
}

func New() *Store {
	return &Store{
		data:     make(map[string]entry),
		volatile: make(map[string]struct{}),
		waiters:  make(map[string][]*waiter),
		now:      time.Now,
	}
}

// Get returns the string at key. It fails with ErrWrongType if key holds
// a list.
func (s *Store) Get(key string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.lookup(key, s.now())
	if !ok {
		return "", false, nil
	}
	v, isString := e.value.(string)
	if !isString {
		return "", false, ErrWrongType
	}
	return v, true, nil
}

// Set stores value at key, replacing whatever was there, list or string.
// A ttl of 0 means no expiry; like Redis, setting a key always discards any
// TTL it had before.
func (s *Store) Set(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := entry{value: value}
	if ttl > 0 {
		e.expiresAt = s.now().Add(ttl)
	}
	s.put(key, e)
}

// MGet looks up all keys under one lock, so the result is a consistent
// snapshot. found[i] reports whether keys[i] holds a string; like Redis,
// a list counts as not found rather than an error.
func (s *Store) MGet(keys ...string) (values []string, found []bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	values = make([]string, len(keys))
	found = make([]bool, len(keys))
	for i, k := range keys {
		if e, ok := s.lookup(k, now); ok {
			values[i], found[i] = e.value.(string)
		}
	}
	return values, found
}

// MSet sets key/value pairs atomically: no reader sees half of them.
// pairs is key1, value1, key2, value2, ...; the caller ensures an even length.
// Like Set, it clears any existing TTLs.
func (s *Store) MSet(pairs ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i+1 < len(pairs); i += 2 {
		s.put(pairs[i], entry{value: pairs[i+1]})
	}
}

// Del removes keys and returns how many existed.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	n := 0
	for _, k := range keys {
		if _, ok := s.lookup(k, now); ok {
			n++
		}
		s.remove(k) // also drops an expired key that lookup ignored
	}
	return n
}

// Exists returns how many of keys exist. Like Redis, a key named twice is
// counted twice.
func (s *Store) Exists(keys ...string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	n := 0
	for _, k := range keys {
		if _, ok := s.lookup(k, now); ok {
			n++
		}
	}
	return n
}

// IncrBy adds delta to the integer stored at key and returns the new value.
// A missing key counts as 0. An existing TTL is kept.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.lookup(key, s.now())
	var n int64
	if ok {
		v, isString := e.value.(string)
		if !isString {
			return 0, ErrWrongType
		}
		var err error
		if n, err = parseInt(v); err != nil {
			return 0, err
		}
	}

	if (delta > 0 && n > math.MaxInt64-delta) || (delta < 0 && n < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	n += delta
	e.value = strconv.FormatInt(n, 10)
	s.put(key, e)
	return n, nil
}

// Expire sets a TTL on an existing key and reports whether the key existed.
// A ttl of zero or less deletes the key straight away, as Redis does.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	e, ok := s.lookup(key, now)
	if !ok {
		s.remove(key)
		return false
	}
	if ttl <= 0 {
		s.remove(key)
		return true
	}
	e.expiresAt = now.Add(ttl)
	s.put(key, e)
	return true
}

// TTL returns the time key has left to live. exists is false if the key is
// missing or expired; ttl is NoExpiry if the key exists but never expires.
func (s *Store) TTL(key string) (ttl time.Duration, exists bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	e, ok := s.lookup(key, now)
	if !ok {
		return 0, false
	}
	if e.expiresAt.IsZero() {
		return NoExpiry, true
	}
	return e.expiresAt.Sub(now), true
}

// Persist removes the TTL from key. It reports whether there was one.
func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.lookup(key, s.now())
	if !ok || e.expiresAt.IsZero() {
		return false
	}
	e.expiresAt = time.Time{}
	s.put(key, e)
	return true
}

// lookup returns the entry for key, treating an expired entry as missing.
// The caller must hold s.mu, for reading at least.
func (s *Store) lookup(key string, now time.Time) (entry, bool) {
	e, ok := s.data[key]
	if !ok || e.expired(now) {
		return entry{}, false
	}
	return e, true
}

// put and remove are the only places that write data, so the volatile
// index can never drift out of sync with it. The caller must hold s.mu.Lock.
func (s *Store) put(key string, e entry) {
	s.data[key] = e
	if e.expiresAt.IsZero() {
		delete(s.volatile, key)
	} else {
		s.volatile[key] = struct{}{}
	}
}

func (s *Store) remove(key string) {
	delete(s.data, key)
	delete(s.volatile, key)
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
