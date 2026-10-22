package store

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Journal interface {
	Append(args ...string)
}

var ErrBadEntry = errors.New("bad journal entry")

func (s *Store) SetJournal(j Journal) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.journal = j
}

func (s *Store) record(args ...string) {
	if s.journal != nil {
		s.journal.Append(args...)
	}
}

func (s *Store) recordSet(key string, e entry) {
	v := e.value.(string)
	if e.expiresAt.IsZero() {
		s.record("SET", key, v)
		return
	}
	s.record("SET", key, v, "PXAT", unixMilli(e.expiresAt))
}

func popName(front bool) string {
	if front {
		return "LPOP"
	}
	return "RPOP"
}

func unixMilli(t time.Time) string {
	return strconv.FormatInt(t.UnixMilli(), 10)
}

func (s *Store) Dump() [][]string {
	s.mu.RLock()
	now := s.now()
	var cmds [][]string
	for key, e := range s.data {
		if e.expired(now) {
			continue
		}
		switch v := e.value.(type) {
		case string:
			cmd := []string{"SET", key, v}
			if !e.expiresAt.IsZero() {
				cmd = append(cmd, "PXAT", unixMilli(e.expiresAt))
			}
			cmds = append(cmds, cmd)
		case *list:
			cmd := make([]string, 0, v.len()+2)
			cmd = append(cmd, "RPUSH", key)
			for i := range v.len() {
				cmd = append(cmd, v.at(i))
			}
			cmds = append(cmds, cmd)
			if !e.expiresAt.IsZero() {
				cmds = append(cmds, []string{"PEXPIREAT", key, unixMilli(e.expiresAt)})
			}
		}
	}
	s.mu.RUnlock()

	slices.SortStableFunc(cmds, func(a, b []string) int { return strings.Compare(a[1], b[1]) })
	return cmds
}

func (s *Store) Apply(args []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(args) < 2 {
		return fmt.Errorf("%w: %q", ErrBadEntry, args)
	}
	key := args[1]
	bad := fmt.Errorf("%w: %q", ErrBadEntry, args)

	switch strings.ToUpper(args[0]) {
	case "SET":
		e := entry{}
		switch {
		case len(args) == 3:
		case len(args) == 5 && strings.EqualFold(args[3], "PXAT"):
			at, err := parseInt(args[4])
			if err != nil {
				return bad
			}
			e.expiresAt = time.UnixMilli(at)
		default:
			return bad
		}
		e.value = args[2]
		s.put(key, e)

	case "MSET":
		if len(args)%2 == 0 {
			return bad
		}
		for i := 1; i+1 < len(args); i += 2 {
			s.put(args[i], entry{value: args[i+1]})
		}

	case "DEL":
		for _, k := range args[1:] {
			s.remove(k)
		}

	case "PEXPIREAT":
		if len(args) != 3 {
			return bad
		}
		at, err := parseInt(args[2])
		if err != nil {
			return bad
		}
		if e, ok := s.data[key]; ok {
			e.expiresAt = time.UnixMilli(at)
			s.put(key, e)
		}

	case "PERSIST":
		if e, ok := s.data[key]; ok {
			e.expiresAt = time.Time{}
			s.put(key, e)
		}

	case "LPUSH", "RPUSH":
		front := strings.EqualFold(args[0], "LPUSH")
		e, ok := s.data[key]
		if !ok {
			e = entry{value: &list{}}
			s.put(key, e)
		}
		l, isList := e.value.(*list)
		if !isList {
			return bad
		}
		for _, v := range args[2:] {
			if front {
				l.pushFront(v)
			} else {
				l.pushBack(v)
			}
		}

	case "LPOP", "RPOP":
		front := strings.EqualFold(args[0], "LPOP")
		count := int64(1)
		if len(args) == 3 {
			n, err := parseInt(args[2])
			if err != nil || n < 0 {
				return bad
			}
			count = n
		}
		e, ok := s.data[key]
		if !ok {
			return nil
		}
		l, isList := e.value.(*list)
		if !isList {
			return bad
		}
		for i := int64(0); i < count && l.len() > 0; i++ {
			l.pop(front)
		}
		s.dropIfEmpty(key, l)

	default:
		return bad
	}
	return nil
}
