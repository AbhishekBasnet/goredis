package stats

import (
	"testing"
	"time"
)

func TestCollect(t *testing.T) {
	c := &Counters{}
	c.ConnectedClients.Store(3)
	c.TotalConnections.Store(10)
	c.RejectedConnections.Store(1)
	c.CommandsProcessed.Store(42)

	r := Sources{
		Counters:   c,
		Start:      time.Now().Add(-90 * time.Second),
		MaxClients: 100,
		AOFEnabled: true,
		Keys:       func() (int, int) { return 7, 2 },
		Blocked:    func() int { return 1 },
		PubSub:     func() (int, int) { return 4, 5 },
		Saving:     func() bool { return true },
	}.Collect()

	want := Report{
		ConnectedClients: 3, TotalConnections: 10, RejectedConnections: 1, CommandsProcessed: 42,
		MaxClients: 100, AOFEnabled: true, Keys: 7, Expires: 2, BlockedClients: 1,
		PubSubClients: 4, PubSubChannels: 5, BGSaveInProgress: true, UptimeSeconds: 90,
	}
	r.ProcessID, r.GoVersion, r.HeapBytes, r.SysBytes, r.Goroutines = 0, "", 0, 0, 0
	if r != want {
		t.Errorf("Collect =\n%+v\nwant\n%+v", r, want)
	}
}

func TestCollectWithNoSources(t *testing.T) {
	r := Sources{}.Collect()
	if r.ProcessID == 0 || r.GoVersion == "" || r.HeapBytes == 0 || r.Goroutines == 0 {
		t.Errorf("runtime fields missing: %+v", r)
	}
	if r.Keys != 0 || r.ConnectedClients != 0 || r.UptimeSeconds != 0 {
		t.Errorf("unset sources produced values: %+v", r)
	}
}
