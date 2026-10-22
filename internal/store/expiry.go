package store

import (
	"context"
	"time"
)

const (
	DefaultExpiryInterval = 100 * time.Millisecond

	expirySample    = 20
	expiryMaxRounds = 16
)

func (s *Store) RunExpiry(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.activeExpire()
		}
	}
}

func (s *Store) activeExpire() {
	for range expiryMaxRounds {
		sampled, expired := s.expireSample(expirySample)
		if sampled == 0 || expired*4 <= sampled {
			return
		}
	}
}

func (s *Store) expireSample(n int) (sampled, expired int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	for key := range s.volatile {
		if sampled == n {
			break
		}
		sampled++
		if s.data[key].expired(now) {
			s.remove(key)
			s.record("DEL", key)
			expired++
		}
	}
	return sampled, expired
}
