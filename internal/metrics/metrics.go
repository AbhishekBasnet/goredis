package metrics

import (
	"fmt"
	"io"
	"net/http"

	"github.com/AbhishekBasnet/goredis/internal/stats"
)

type metric struct {
	name, kind, help string
	value            float64
}

func Handler(collect func() stats.Report) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		write(w, collect())
	})
}

func write(w io.Writer, r stats.Report) {
	for _, m := range []metric{
		{"goredis_uptime_seconds", "gauge", "Seconds since the server started.", float64(r.UptimeSeconds)},
		{"goredis_connected_clients", "gauge", "Clients connected right now.", float64(r.ConnectedClients)},
		{"goredis_blocked_clients", "gauge", "Clients waiting in a blocking command.", float64(r.BlockedClients)},
		{"goredis_pubsub_clients", "gauge", "Clients subscribed to at least one channel.", float64(r.PubSubClients)},
		{"goredis_pubsub_channels", "gauge", "Channels with at least one subscriber.", float64(r.PubSubChannels)},
		{"goredis_max_clients", "gauge", "Connection limit, 0 if unlimited.", float64(r.MaxClients)},
		{"goredis_connections_total", "counter", "Connections accepted.", float64(r.TotalConnections)},
		{"goredis_rejected_connections_total", "counter", "Connections refused because of the client limit.", float64(r.RejectedConnections)},
		{"goredis_commands_processed_total", "counter", "Commands run.", float64(r.CommandsProcessed)},
		{"goredis_keys", "gauge", "Keys in the dataset.", float64(r.Keys)},
		{"goredis_keys_with_expiry", "gauge", "Keys that have a TTL.", float64(r.Expires)},
		{"goredis_heap_bytes", "gauge", "Bytes of allocated heap objects.", float64(r.HeapBytes)},
		{"goredis_sys_bytes", "gauge", "Bytes obtained from the OS.", float64(r.SysBytes)},
		{"goredis_goroutines", "gauge", "Goroutines that currently exist.", float64(r.Goroutines)},
		{"goredis_bgsave_in_progress", "gauge", "1 while a snapshot is being written.", boolFloat(r.BGSaveInProgress)},
	} {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %g\n", m.name, m.help, m.name, m.kind, m.name, m.value)
	}
}

func boolFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
