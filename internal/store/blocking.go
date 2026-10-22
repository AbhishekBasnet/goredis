package store

import (
	"context"
	"slices"
	"time"
)

// Popped is one element taken from a list by a blocking pop.
type Popped struct {
	Key, Value string
}

// waiter is one client blocked in BLPOP or BRPOP. It is queued on every key
// it waits for, and served at most once.
type waiter struct {
	keys  []string
	front bool

	// ch has room for exactly one value, so a pusher can hand the value over
	// while holding the lock without ever blocking.
	ch chan Popped
}

// BPop pops from the front (BLPOP) or back (BRPOP) of the first non-empty
// list among keys, checked in order. If they are all empty it waits until
// another client pushes to one of them, timeout passes, or ctx is done.
// A timeout of 0 waits forever.
//
// ok is false if it gave up without a value. err is ErrWrongType if a key
// holds a string.
//
// Values are handed over, not raced for: a push gives its element straight
// to the longest-waiting client, under the lock. A woken client never has to
// compete with others to pop, so no element is lost or delivered twice.
func (s *Store) BPop(ctx context.Context, keys []string, front bool, timeout time.Duration) (p Popped, ok bool, err error) {
	s.mu.Lock()
	for _, k := range keys {
		l, found, err := s.listAt(k)
		if err != nil {
			s.mu.Unlock()
			return Popped{}, false, err
		}
		if found { // never empty: dropIfEmpty deletes empty lists
			p = Popped{Key: k, Value: l.pop(front)}
			s.dropIfEmpty(k, l)
			s.mu.Unlock()
			return p, true, nil
		}
	}

	// Nothing to pop: queue up on every key, then wait without the lock.
	w := &waiter{keys: keys, front: front, ch: make(chan Popped, 1)}
	for _, k := range keys {
		s.waiters[k] = append(s.waiters[k], w)
	}
	s.mu.Unlock()

	var expired <-chan time.Time // nil, so never ready, when timeout is 0
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		expired = t.C
	}

	select {
	case p := <-w.ch:
		return p, true, nil
	case <-expired:
	case <-ctx.Done():
	}

	// Giving up. A push may have served us after the timer fired but before
	// we got the lock back; that value is ours now and must not be dropped.
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case p := <-w.ch:
		return p, true, nil
	default:
		s.unregister(w)
		return Popped{}, false, nil
	}
}

// serveWaiters hands elements of key's list to blocked clients, oldest
// first, until the list or the queue runs out. The caller must hold s.mu.Lock.
func (s *Store) serveWaiters(key string, l *list) {
	for l.len() > 0 && len(s.waiters[key]) > 0 {
		w := s.waiters[key][0]
		s.unregister(w) // from all its keys, so it can't be served twice
		w.ch <- Popped{Key: key, Value: l.pop(w.front)}
	}
	s.dropIfEmpty(key, l)
}

// unregister removes w from the queue of every key it waits on.
// The caller must hold s.mu.Lock.
func (s *Store) unregister(w *waiter) {
	for _, k := range w.keys {
		q := slices.DeleteFunc(s.waiters[k], func(x *waiter) bool { return x == w })
		if len(q) == 0 {
			delete(s.waiters, k)
		} else {
			s.waiters[k] = q
		}
	}
}
