package store

import (
	"context"
	"slices"
	"time"
)

type Popped struct {
	Key, Value string
}

type waiter struct {
	keys  []string
	front bool
	ch    chan Popped
}

func (s *Store) BPop(ctx context.Context, keys []string, front bool, timeout time.Duration) (p Popped, ok bool, err error) {
	s.mu.Lock()
	for _, k := range keys {
		l, found, err := s.listAt(k)
		if err != nil {
			s.mu.Unlock()
			return Popped{}, false, err
		}
		if found {
			p = Popped{Key: k, Value: l.pop(front)}
			s.record(popName(front), k)
			s.dropIfEmpty(k, l)
			s.mu.Unlock()
			return p, true, nil
		}
	}

	w := &waiter{keys: keys, front: front, ch: make(chan Popped, 1)}
	for _, k := range keys {
		s.waiters[k] = append(s.waiters[k], w)
	}
	s.mu.Unlock()

	var expired <-chan time.Time
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

func (s *Store) serveWaiters(key string, l *list) {
	for l.len() > 0 && len(s.waiters[key]) > 0 {
		w := s.waiters[key][0]
		s.unregister(w)
		w.ch <- Popped{Key: key, Value: l.pop(w.front)}
		s.record(popName(w.front), key)
	}
	s.dropIfEmpty(key, l)
}

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
