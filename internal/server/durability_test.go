package server

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

type gatedLog struct {
	seq     atomic.Uint64
	waits   atomic.Int64
	release chan struct{}
}

func (g *gatedLog) Append(args ...string) { g.seq.Add(1) }
func (g *gatedLog) Seq() uint64           { return g.seq.Load() }
func (g *gatedLog) WaitFor(uint64) error {
	g.waits.Add(1)
	<-g.release
	return nil
}

func serveWithLog(t *testing.T) (*running, *gatedLog) {
	t.Helper()
	st := store.New()
	log := &gatedLog{release: make(chan struct{})}
	st.SetJournal(log)
	reg := newRegistry(st)
	r := serve(t, Config{AppendLog: log}, func(w *resp.Writer) Session { return reg.NewClient(w) })
	return r, log
}

func expectNothingFor(t *testing.T, c net.Conn, d time.Duration) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(d))
	buf := make([]byte, 64)
	if n, err := c.Read(buf); err == nil {
		t.Fatalf("got %q before the AOF entry was durable", buf[:n])
	}
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
}

func TestWriteReplyWaitsForDurability(t *testing.T) {
	r, log := serveWithLog(t)
	writer, reader := dialServer(t, r.addr), dialServer(t, r.addr)

	send(writer, "SET k v\r\n")
	deadline := time.Now().Add(time.Second)
	for log.Seq() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("SET never reached the journal")
		}
		time.Sleep(time.Millisecond)
	}
	expectNothingFor(t, writer, 100*time.Millisecond)

	send(reader, "GET k\r\n")
	readExactly(t, reader, "$1\r\nv\r\n")

	close(log.release)
	readExactly(t, writer, "+OK\r\n")
}

func TestPipelinedWritesWaitOnce(t *testing.T) {
	r, log := serveWithLog(t)
	close(log.release)
	c := dialServer(t, r.addr)

	send(c, "SET a 1\r\nSET b 2\r\nINCR n\r\nGET a\r\n")
	readExactly(t, c, "+OK\r\n+OK\r\n:1\r\n$1\r\n1\r\n")
	if n := log.waits.Load(); n != 1 {
		t.Errorf("WaitFor called %d times for one pipelined batch, want 1", n)
	}

	send(c, "GET a\r\nPING\r\n")
	readExactly(t, c, "$1\r\n1\r\n+PONG\r\n")
	if n := log.waits.Load(); n != 1 {
		t.Errorf("a read-only batch waited on the AOF (%d waits)", n)
	}
}

func TestProtocolErrorReplyWaitsForEarlierWrite(t *testing.T) {
	r, log := serveWithLog(t)
	c := dialServer(t, r.addr)

	send(c, "SET a 1\r\n*x\r\n")
	expectNothingFor(t, c, 100*time.Millisecond)
	close(log.release)
	readExactly(t, c, "+OK\r\n-ERR Protocol error: invalid multibulk length\r\n")
}
