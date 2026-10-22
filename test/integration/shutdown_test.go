package integration

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestShutdownUnderWriteLoadKeepsAcknowledgedWrites(t *testing.T) {
	const writers = 8
	dir := t.TempDir()
	p := startBinary(t, dir)

	acked := make([]int, writers)
	var wg sync.WaitGroup
	for i := range writers {
		conn, err := net.Dial("tcp", p.addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		wg.Go(func() {
			r := bufio.NewReader(conn)
			for n := 0; ; n++ {
				key, val := fmt.Sprintf("w%d-%d", i, n), fmt.Sprint(n)
				fmt.Fprintf(conn, "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
				line, err := r.ReadString('\n')
				if err != nil || line != "+OK\r\n" {
					return
				}
				acked[i] = n + 1
			}
		})
	}

	time.Sleep(300 * time.Millisecond)
	p.cmd.Process.Signal(syscall.SIGTERM)
	wg.Wait()
	if err := p.cmd.Wait(); err != nil {
		t.Fatalf("exit: %v\n%s", err, p.logs)
	}
	for _, msg := range []string{"draining clients", "all clients disconnected", "shutdown complete"} {
		if !strings.Contains(p.logs.String(), msg) {
			t.Errorf("log is missing %q", msg)
		}
	}

	total := 0
	for _, n := range acked {
		total += n
	}
	if total < writers {
		t.Fatalf("only %d writes were acknowledged; the test did not exercise anything", total)
	}

	p = startBinary(t, dir)
	c := dial(t, p.addr)
	for i, n := range acked {
		if n == 0 {
			continue
		}
		for k := max(0, n-50); k < n; k++ {
			if got, want := c.do("GET", fmt.Sprintf("w%d-%d", i, k)), fmt.Sprintf(`"%d"`, k); got != want {
				t.Errorf("writer %d, acknowledged write %d of %d: got %s", i, k, n, got)
			}
		}
	}
	p.stop(t)
}

func TestShutdownWithBlockedAndSubscribedClients(t *testing.T) {
	p := startBinary(t, t.TempDir())
	blocked, sub := dial(t, p.addr), dial(t, p.addr)
	sub.do("SUBSCRIBE", "news")
	blocked.send("BLPOP", "jobs", "0")
	time.Sleep(50 * time.Millisecond)

	start := time.Now()
	p.stop(t)
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("shutdown took %v with a blocked client and a subscriber", d)
	}
	if got := blocked.read(); got != "(nil)" {
		t.Errorf("blocked client got %s, want (nil)", got)
	}
	if _, err := sub.r.ReadString('\n'); err != io.EOF {
		t.Errorf("subscriber read %v, want EOF", err)
	}
}

func TestMaxClientsFlag(t *testing.T) {
	p := startBinary(t, t.TempDir(), "-maxclients", "1")
	first := dial(t, p.addr)
	expect(t, first, "PONG", "PING")

	second := dial(t, p.addr)
	if got := second.read(); got != "(error) ERR max number of clients reached" {
		t.Errorf("second client got %s", got)
	}
	p.stop(t)
}

func TestInfoAndMetrics(t *testing.T) {
	p := startBinary(t, t.TempDir(), "-metrics-addr", "127.0.0.1:0", "-maxclients", "50")
	c := dial(t, p.addr)
	expect(t, c, "OK", "SET", "a", "1")
	expect(t, c, "OK", "SET", "b", "2", "EX", "100")

	info := c.do("INFO")
	for _, want := range []string{"connected_clients:1", "maxclients:50", "aof_enabled:1", "db0:keys=2,expires=1"} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO is missing %q:\n%s", want, info)
		}
	}

	metricsAddr := p.logged("metrics listening")
	if metricsAddr == "" {
		t.Fatalf("no metrics address in the log:\n%s", p.logs)
	}
	res, err := http.Get("http://" + metricsAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	for _, want := range []string{"goredis_connected_clients 1\n", "goredis_keys 2\n", "goredis_max_clients 50\n"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics are missing %q:\n%s", want, body)
		}
	}
	p.stop(t)
}
