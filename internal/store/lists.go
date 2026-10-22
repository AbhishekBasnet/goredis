package store

import "strconv"

func (s *Store) Push(key string, front bool, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, ok, err := s.listAt(key)
	if err != nil {
		return 0, err
	}
	if !ok {
		if _, stale := s.data[key]; stale {
			s.record("DEL", key)
		}
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
	pushName := "RPUSH"
	if front {
		pushName = "LPUSH"
	}
	s.record(append([]string{pushName, key}, values...)...)

	n := l.len()
	s.serveWaiters(key, l)
	return n, nil
}

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
	if len(values) > 0 {
		s.record(popName(front), key, strconv.Itoa(len(values)))
	}
	s.dropIfEmpty(key, l)
	return values, true, nil
}

func (s *Store) LLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	l, ok, err := s.listAt(key)
	if err != nil || !ok {
		return 0, err
	}
	return l.len(), nil
}

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

func (s *Store) dropIfEmpty(key string, l *list) {
	if l.len() == 0 {
		s.remove(key)
	}
}
