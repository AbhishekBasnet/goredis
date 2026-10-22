package store

// Push adds values to the front (LPUSH) or back (RPUSH) of the list at key,
// creating the list if needed, and returns its new length. Values go in one
// at a time, so LPUSH k a b c leaves the list as c b a.
//
// Clients blocked on key are then served from the list, oldest first, so
// the returned length counts elements they may already have taken, as in
// Redis.
func (s *Store) Push(key string, front bool, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, ok, err := s.listAt(key)
	if err != nil {
		return 0, err
	}
	if !ok {
		l = &list{}
		s.put(key, entry{value: l})
	}

	for _, v := range values {
		if front {
			l.pushFront(v)
		} else {
			l.pushBack(v)
		}
	}
	n := l.len()
	s.serveWaiters(key, l)
	return n, nil
}

// Pop removes up to count elements from the front (LPOP) or back (RPOP) of
// the list at key. exists is false if there is no such key.
func (s *Store) Pop(key string, front bool, count int) (values []string, exists bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, ok, err := s.listAt(key)
	if err != nil || !ok {
		return nil, false, err
	}

	values = make([]string, min(count, l.len()))
	for i := range values {
		values[i] = l.pop(front)
	}
	s.dropIfEmpty(key, l)
	return values, true, nil
}

// LLen returns the length of the list at key, 0 if there is none.
func (s *Store) LLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	l, ok, err := s.listAt(key)
	if err != nil || !ok {
		return 0, err
	}
	return l.len(), nil
}

// LRange returns the elements from start to stop, both inclusive. Negative
// indexes count from the end (-1 is the last element), and out-of-range
// indexes are clamped rather than being an error, as in Redis.
func (s *Store) LRange(key string, start, stop int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	l, ok, err := s.listAt(key)
	if err != nil || !ok {
		return nil, err
	}

	n := l.len()
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	start, stop = max(start, 0), min(stop, n-1)
	if start > stop {
		return nil, nil
	}

	values := make([]string, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		values = append(values, l.at(i))
	}
	return values, nil
}

// listAt returns the list at key. ok is false if the key does not exist;
// err is ErrWrongType if it holds a string. The caller must hold s.mu.
func (s *Store) listAt(key string) (l *list, ok bool, err error) {
	e, found := s.lookup(key, s.now())
	if !found {
		return nil, false, nil
	}
	l, isList := e.value.(*list)
	if !isList {
		return nil, false, ErrWrongType
	}
	return l, true, nil
}

// dropIfEmpty deletes key once its list is empty: Redis never keeps an empty
// list, so EXISTS and TYPE behave as if it was never there.
// The caller must hold s.mu.Lock.
func (s *Store) dropIfEmpty(key string, l *list) {
	if l.len() == 0 {
		s.remove(key)
	}
}
