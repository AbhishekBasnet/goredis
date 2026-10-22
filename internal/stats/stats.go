package stats

import (
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

type Counters struct {
	ConnectedClients    atomic.Int64
	TotalConnections    atomic.Int64
	RejectedConnections atomic.Int64
	CommandsProcessed   atomic.Int64
}

type Report struct {
	ProcessID     int
	GoVersion     string
	UptimeSeconds int64

	ConnectedClients int64
	BlockedClients   int64
	PubSubClients    int64
	MaxClients       int64

	TotalConnections    int64
	RejectedConnections int64
	CommandsProcessed   int64
	PubSubChannels      int64

	HeapBytes  uint64
	SysBytes   uint64
	Goroutines int

	AOFEnabled       bool
	BGSaveInProgress bool

	Keys    int64
	Expires int64
}

type Sources struct {
	Counters   *Counters
	Start      time.Time
	MaxClients int
	AOFEnabled bool
	Keys       func() (keys, expires int)
	Blocked    func() int
	PubSub     func() (clients, channels int)
	Saving     func() bool
}

func (s Sources) Collect() Report {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	r := Report{
		ProcessID:  os.Getpid(),
		GoVersion:  runtime.Version(),
		MaxClients: int64(s.MaxClients),
		HeapBytes:  mem.HeapAlloc,
		SysBytes:   mem.Sys,
		Goroutines: runtime.NumGoroutine(),
		AOFEnabled: s.AOFEnabled,
	}
	if !s.Start.IsZero() {
		r.UptimeSeconds = int64(time.Since(s.Start) / time.Second)
	}
	if c := s.Counters; c != nil {
		r.ConnectedClients = c.ConnectedClients.Load()
		r.TotalConnections = c.TotalConnections.Load()
		r.RejectedConnections = c.RejectedConnections.Load()
		r.CommandsProcessed = c.CommandsProcessed.Load()
	}
	if s.Keys != nil {
		keys, expires := s.Keys()
		r.Keys, r.Expires = int64(keys), int64(expires)
	}
	if s.Blocked != nil {
		r.BlockedClients = int64(s.Blocked())
	}
	if s.PubSub != nil {
		clients, channels := s.PubSub()
		r.PubSubClients, r.PubSubChannels = int64(clients), int64(channels)
	}
	if s.Saving != nil {
		r.BGSaveInProgress = s.Saving()
	}
	return r
}
