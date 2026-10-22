package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AbhishekBasnet/goredis/internal/stats"
)

func TestHandler(t *testing.T) {
	report := stats.Report{ConnectedClients: 3, CommandsProcessed: 1234567, Keys: 10, BGSaveInProgress: true}
	srv := httptest.NewServer(Handler(func() stats.Report { return report }))
	defer srv.Close()

	res, err := srv.Client().Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Errorf("Content-Type = %q", ct)
	}
	for _, want := range []string{
		"# TYPE goredis_connected_clients gauge\ngoredis_connected_clients 3\n",
		"# TYPE goredis_commands_processed_total counter\ngoredis_commands_processed_total 1.234567e+06\n",
		"goredis_keys 10\n",
		"goredis_bgsave_in_progress 1\n",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}

	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if fields := strings.Fields(line); len(fields) != 2 || !strings.HasPrefix(fields[0], "goredis_") {
			t.Errorf("malformed sample line %q", line)
		}
	}
}
