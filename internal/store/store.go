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

const NoExpiry time.Duration = -1

type entry struct {
	value     any
	expiresAt time.Time
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

type Store struct {
	mu       sync.RWMutex
	data     map[string]entry
	volatile map[string]struct{}
	waiters  map[string][]*waiter
	now      func() time.Time
}

func New() *Store {
	return &Store{
		data:     make(map[string]entry),
		volatile: make(map[string]struct{}),
		waiters:  make(map[string][]*waiter),
		now:      time.Now,
	}
}

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

func (s *Store) Set(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := entry{value: value}
	if ttl > 0 {
		e.expiresAt = s.now().Add(ttl)
	}
	s.put(key, e)
}

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

func (s *Store) MSet(pairs ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 0; i+1 < len(pairs); i += 2 {
		s.put(pairs[i], entry{value: pairs[i+1]})
	}
}

func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	n := 0
	for _, k := range keys {
		if _, ok := s.lookup(k, now); ok {
			n++
		}
		s.remove(k)
	}
	return n
}

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

func (s *Store) lookup(key string, now time.Time) (entry, bool) {
	e, ok := s.data[key]
	if !ok || e.expired(now) {
		return entry{}, false
	}
	return e, true
}

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

func parseInt(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != s {
		return 0, ErrNotInteger
	}
	return n, nil
}
