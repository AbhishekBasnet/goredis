package command

import (
	"strings"
	"testing"

	"github.com/AbhishekBasnet/goredis/internal/stats"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

func infoBody(t *testing.T, c *testClient, args ...string) string {
	t.Helper()
	reply := c.run(append([]string{"INFO"}, args...)...)
	header, body, ok := strings.Cut(reply, "\r\n")
	if !ok || !strings.HasPrefix(header, "$") {
		t.Fatalf("INFO reply is not a bulk string: %q", reply)
	}
	return strings.TrimSuffix(body, "\r\n")
}

func TestInfo(t *testing.T) {
	report := stats.Report{
		ProcessID: 4242, GoVersion: "go1.26", UptimeSeconds: 200000,
		ConnectedClients: 3, BlockedClients: 1, PubSubClients: 2, MaxClients: 100,
		TotalConnections: 9, RejectedConnections: 1, CommandsProcessed: 77, PubSubChannels: 4,
		HeapBytes: 3 << 20, AOFEnabled: true, Keys: 7, Expires: 2,
	}
	c := newTestClient(NewRegistry(Config{Store: store.New(), Info: func() stats.Report { return report }}))

	body := infoBody(t, c)
	for _, want := range []string{
		"# Server\r\n", "process_id:4242\r\n", "uptime_in_days:2\r\n",
		"# Clients\r\nconnected_clients:3\r\nblocked_clients:1\r\npubsub_clients:2\r\nmaxclients:100\r\n",
		"used_memory:3145728\r\nused_memory_human:3.00M\r\n",
		"aof_enabled:1\r\nrdb_bgsave_in_progress:0\r\n",
		"total_commands_processed:77\r\nrejected_connections:1\r\n",
		"# Keyspace\r\ndb0:keys=7,expires=2,avg_ttl=0",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("INFO is missing %q:\n%s", want, body)
		}
	}

	only := infoBody(t, c, "CLIENTS", "keyspace")
	if !strings.HasPrefix(only, "# Clients\r\n") || !strings.Contains(only, "# Keyspace") || strings.Contains(only, "# Server") {
		t.Errorf("INFO clients keyspace =\n%s", only)
	}
	if got := infoBody(t, c, "nonsense"); got != "" {
		t.Errorf("INFO nonsense = %q, want empty", got)
	}
}

func TestInfoDefaultsCountCommands(t *testing.T) {
	st := store.New()
	c := newTestClient(NewRegistry(Config{Store: st}))
	c.run("SET", "a", "1")
	c.run("SET", "b", "2")
	c.run("NOPE")

	body := infoBody(t, c, "stats", "keyspace")
	if !strings.Contains(body, "total_commands_processed:3\r\n") {
		t.Errorf("want 2 SETs plus this INFO counted:\n%s", body)
	}
	if !strings.Contains(body, "db0:keys=2,expires=0") {
		t.Errorf("keyspace not taken from the store:\n%s", body)
	}
}

func TestHumanBytes(t *testing.T) {
	for n, want := range map[uint64]string{0: "0B", 1023: "1023B", 1536: "1.50K", 5 << 20: "5.00M", 3 << 30: "3.00G"} {
		if got := humanBytes(n); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", n, got, want)
		}
	}
}
