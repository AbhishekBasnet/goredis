package store

import (
	"context"
	"time"
)

// Active expiry settings, the same as Redis's defaults: every 100ms, sample
// 20 keys that have a TTL. If more than a quarter of them had expired,
// there are probably many more, so sample again straight away.
const (
	DefaultExpiryInterval = 100 * time.Millisecond

	expirySample    = 20
	expiryMaxRounds = 16 // bounds the work done in one tick
)

// RunExpiry deletes expired keys in the background until ctx is cancelled.
// Run it in its own goroutine; it returns once the context is done, so the
// caller can wait for it during shutdown.
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

// activeExpire runs sampling rounds until one finds few expired keys.
func (s *Store) activeExpire() {
	for range expiryMaxRounds {
		sampled, expired := s.expireSample(expirySample)
		if sampled == 0 || expired*4 <= sampled {
			return
		}
	}
}

// expireSample checks up to n keys that have a TTL and deletes the expired
// ones. The lock is taken once per round, not once per tick, so clients
// get a turn between rounds.
//
// Go randomises where each range over a map starts, which is what makes
// stopping after n keys a sample rather than always the same n keys.
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
			s.remove(key) // deleting during range is allowed in Go
			expired++
		}
	}
	return sampled, expired
}
