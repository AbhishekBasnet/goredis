package command

import (
	"context"
	"fmt"
	"strings"

	"github.com/AbhishekBasnet/goredis/internal/stats"
)

type infoSection struct {
	name  string
	lines func(r stats.Report) []string
}

var infoSections = []infoSection{
	{"server", func(r stats.Report) []string {
		return []string{
			"redis_mode:standalone",
			fmt.Sprintf("process_id:%d", r.ProcessID),
			"go_version:" + r.GoVersion,
			fmt.Sprintf("goroutines:%d", r.Goroutines),
			fmt.Sprintf("uptime_in_seconds:%d", r.UptimeSeconds),
			fmt.Sprintf("uptime_in_days:%d", r.UptimeSeconds/86400),
		}
	}},
	{"clients", func(r stats.Report) []string {
		return []string{
			fmt.Sprintf("connected_clients:%d", r.ConnectedClients),
			fmt.Sprintf("blocked_clients:%d", r.BlockedClients),
			fmt.Sprintf("pubsub_clients:%d", r.PubSubClients),
			fmt.Sprintf("maxclients:%d", r.MaxClients),
		}
	}},
	{"memory", func(r stats.Report) []string {
		return []string{
			fmt.Sprintf("used_memory:%d", r.HeapBytes),
			"used_memory_human:" + humanBytes(r.HeapBytes),
			fmt.Sprintf("used_memory_from_os:%d", r.SysBytes),
		}
	}},
	{"persistence", func(r stats.Report) []string {
		return []string{
			fmt.Sprintf("aof_enabled:%d", boolInt(r.AOFEnabled)),
			fmt.Sprintf("rdb_bgsave_in_progress:%d", boolInt(r.BGSaveInProgress)),
		}
	}},
	{"stats", func(r stats.Report) []string {
		return []string{
			fmt.Sprintf("total_connections_received:%d", r.TotalConnections),
			fmt.Sprintf("total_commands_processed:%d", r.CommandsProcessed),
			fmt.Sprintf("rejected_connections:%d", r.RejectedConnections),
			fmt.Sprintf("pubsub_channels:%d", r.PubSubChannels),
		}
	}},
	{"keyspace", func(r stats.Report) []string {
		if r.Keys == 0 {
			return nil
		}
		return []string{fmt.Sprintf("db0:keys=%d,expires=%d,avg_ttl=0", r.Keys, r.Expires)}
	}},
}

func info(_ context.Context, c *Client, args []string) {
	wanted := make(map[string]bool)
	for _, a := range args[1:] {
		wanted[strings.ToLower(a)] = true
	}
	all := len(wanted) == 0 || wanted["all"] || wanted["default"] || wanted["everything"]

	r := c.reg.info()
	var b strings.Builder
	for _, s := range infoSections {
		if !all && !wanted[s.name] {
			continue
		}
		if b.Len() > 0 {
			b.WriteString("\r\n")
		}
		b.WriteString("# " + strings.ToUpper(s.name[:1]) + s.name[1:] + "\r\n")
		for _, line := range s.lines(r) {
			b.WriteString(line + "\r\n")
		}
	}
	c.w.WriteBulk(b.String())
}

func humanBytes(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2fG", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2fM", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2fK", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%dB", n)
}
