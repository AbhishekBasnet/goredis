package integration

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	buildOnce sync.Once
	buildDir  string
	binPath   string
	buildErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if buildDir != "" {
		os.RemoveAll(buildDir)
	}
	os.Exit(code)
}

func binary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		if buildDir, buildErr = os.MkdirTemp("", "goredis-test-"); buildErr != nil {
			return
		}
		binPath = filepath.Join(buildDir, "goredis")
		out, err := exec.Command("go", "build", "-o", binPath, "github.com/AbhishekBasnet/goredis/cmd/goredis").CombinedOutput()
		if err != nil {
			buildErr = err
			binPath = string(out)
		}
	})
	if buildErr != nil {
		t.Fatalf("build: %v\n%s", buildErr, binPath)
	}
	return binPath
}

type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

type process struct {
	cmd  *exec.Cmd
	logs *logBuffer
	addr string
}

func startBinary(t *testing.T, dir string, flags ...string) *process {
	t.Helper()
	logs := &logBuffer{}
	cmd := exec.Command(binary(t), append([]string{"-addr", "127.0.0.1:0", "-dir", dir}, flags...)...)
	cmd.Stderr = logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &process{cmd: cmd, logs: logs}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for p.addr == "" {
		for _, line := range strings.Split(logs.String(), "\n") {
			var entry struct{ Msg, Addr string }
			if json.Unmarshal([]byte(line), &entry) == nil && entry.Msg == "listening" {
				p.addr = entry.Addr
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("server did not start:\n%s", logs)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return p
}

func (p *process) stop(t *testing.T) {
	t.Helper()
	p.cmd.Process.Signal(syscall.SIGTERM)
	if err := p.cmd.Wait(); err != nil {
		t.Fatalf("server exited with %v:\n%s", err, p.logs)
	}
}

func (p *process) kill(t *testing.T) {
	t.Helper()
	p.cmd.Process.Kill()
	p.cmd.Wait()
}

func expect(t *testing.T, c *client, want string, args ...string) {
	t.Helper()
	if got := c.do(args...); got != want {
		t.Errorf("%q: got %s, want %s", args, got, want)
	}
}

func ttlIn(t *testing.T, c *client, key string, lo, hi int) {
	t.Helper()
	got := c.do("TTL", key)
	n, err := strconv.Atoi(strings.TrimPrefix(got, "(integer) "))
	if err != nil || n < lo || n > hi {
		t.Errorf("TTL %s = %s, want between %d and %d", key, got, lo, hi)
	}
}

func TestDataSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	p := startBinary(t, dir, "-appendfsync", "always")
	c := dial(t, p.addr)
	expect(t, c, "OK", "SET", "s", "hello")
	expect(t, c, "OK", "SET", "t", "temp", "EX", "100")
	expect(t, c, "OK", "SET", "gone", "x")
	expect(t, c, "(integer) 1", "DEL", "gone")
	for i := 1; i <= 3; i++ {
		expect(t, c, "(integer) "+strconv.Itoa(i), "INCR", "n")
	}
	expect(t, c, "OK", "MSET", "a", "1", "b", "2")
	expect(t, c, "(integer) 3", "RPUSH", "l", "x", "y", "z")
	expect(t, c, `"x"`, "LPOP", "l")
	expect(t, c, "(integer) 1", "EXPIRE", "l", "200")
	expect(t, c, "OK", "SET", "p", "v", "EX", "50")
	expect(t, c, "(integer) 1", "PERSIST", "p")
	p.stop(t)

	p = startBinary(t, dir)
	c = dial(t, p.addr)
	expect(t, c, `"hello"`, "GET", "s")
	expect(t, c, "(nil)", "GET", "gone")
	expect(t, c, `"3"`, "GET", "n")
	expect(t, c, `["1" "2"]`, "MGET", "a", "b")
	expect(t, c, `["y" "z"]`, "LRANGE", "l", "0", "-1")
	expect(t, c, "(integer) -1", "TTL", "p")
	ttlIn(t, c, "t", 95, 100)
	ttlIn(t, c, "l", 195, 200)
	p.stop(t)
}

func TestKillDashNineKeepsAcknowledgedWrites(t *testing.T) {
	dir := t.TempDir()
	p := startBinary(t, dir)
	c := dial(t, p.addr)
	for i := range 100 {
		expect(t, c, "OK", "SET", "k"+strconv.Itoa(i), strconv.Itoa(i))
	}
	p.kill(t)

	p = startBinary(t, dir)
	c = dial(t, p.addr)
	expect(t, c, `"0"`, "GET", "k0")
	expect(t, c, `"99"`, "GET", "k99")
	p.stop(t)
}

func TestServedBlockingPopSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	p := startBinary(t, dir)
	waiting, pusher := dial(t, p.addr), dial(t, p.addr)

	waiting.send("BLPOP", "jobs", "0")
	time.Sleep(50 * time.Millisecond)
	expect(t, pusher, "(integer) 2", "RPUSH", "jobs", "a", "b")
	if got := waiting.read(); got != `["jobs" "a"]` {
		t.Fatalf("BLPOP = %s", got)
	}
	expect(t, pusher, "(integer) 2", "LPUSH", "jobs", "z")
	p.stop(t)

	p = startBinary(t, dir)
	c := dial(t, p.addr)
	expect(t, c, `["z" "b"]`, "LRANGE", "jobs", "0", "-1")
	p.stop(t)
}

func TestTruncatedAOFIsRepaired(t *testing.T) {
	dir := t.TempDir()
	complete := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*3\r\n$5\r\nRPUSH\r\n$1\r\nl\r\n$1\r\nx\r\n"
	partial := "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1"
	if err := os.WriteFile(filepath.Join(dir, "appendonly.aof"), []byte(complete+partial), 0o644); err != nil {
		t.Fatal(err)
	}

	p := startBinary(t, dir)
	if !strings.Contains(p.logs.String(), "truncated it") {
		t.Errorf("no truncation warning in the log:\n%s", p.logs)
	}
	c := dial(t, p.addr)
	expect(t, c, `"1"`, "GET", "a")
	expect(t, c, "(nil)", "GET", "b")
	expect(t, c, `["x"]`, "LRANGE", "l", "0", "-1")
	expect(t, c, "OK", "SET", "c", "3")
	p.stop(t)

	p = startBinary(t, dir)
	c = dial(t, p.addr)
	expect(t, c, `["1" (nil) "3"]`, "MGET", "a", "b", "c")
	p.stop(t)
}

func TestCorruptAOFRefusesToStart(t *testing.T) {
	dir := t.TempDir()
	content := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*oops\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"
	if err := os.WriteFile(filepath.Join(dir, "appendonly.aof"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	logs := &logBuffer{}
	cmd := exec.Command(binary(t), "-addr", "127.0.0.1:0", "-dir", dir)
	cmd.Stderr = logs
	err := cmd.Run()
	if err == nil {
		t.Fatal("server started on a corrupt AOF")
	}
	if !strings.Contains(logs.String(), "loading data failed") {
		t.Errorf("log does not explain the failure:\n%s", logs)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "appendonly.aof"))
	if string(data) != content {
		t.Error("a corrupt AOF was modified")
	}
}

func TestSnapshots(t *testing.T) {
	dir := t.TempDir()
	p := startBinary(t, dir, "-appendonly=false")
	c := dial(t, p.addr)
	expect(t, c, "OK", "SET", "a", "1")
	expect(t, c, "(integer) 2", "RPUSH", "l", "x", "y")
	expect(t, c, "OK", "SET", "t", "v", "EX", "100")
	expect(t, c, "OK", "SAVE")
	expect(t, c, "OK", "SET", "b", "2")
	expect(t, c, "Background saving started", "BGSAVE")
	expect(t, c, "OK", "SET", "after", "bgsave")
	p.stop(t)

	if _, err := os.Stat(filepath.Join(dir, "appendonly.aof")); err == nil {
		t.Error("an AOF was written with -appendonly=false")
	}

	p = startBinary(t, dir, "-appendonly=false")
	c = dial(t, p.addr)
	expect(t, c, `["1" "2" (nil)]`, "MGET", "a", "b", "after")
	expect(t, c, `["x" "y"]`, "LRANGE", "l", "0", "-1")
	ttlIn(t, c, "t", 95, 100)
	p.stop(t)
}

func TestEnablingAOFKeepsSnapshotData(t *testing.T) {
	dir := t.TempDir()
	p := startBinary(t, dir, "-appendonly=false")
	c := dial(t, p.addr)
	expect(t, c, "OK", "SET", "from", "snapshot")
	expect(t, c, "OK", "SAVE")
	p.stop(t)

	p = startBinary(t, dir)
	c = dial(t, p.addr)
	expect(t, c, `"snapshot"`, "GET", "from")
	expect(t, c, "OK", "SET", "from2", "aof")
	p.stop(t)

	if err := os.Remove(filepath.Join(dir, "dump.resp")); err != nil {
		t.Fatal(err)
	}
	p = startBinary(t, dir)
	c = dial(t, p.addr)
	expect(t, c, `["snapshot" "aof"]`, "MGET", "from", "from2")
	p.stop(t)
}
