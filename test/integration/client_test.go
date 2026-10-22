package integration

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/command"
	"github.com/AbhishekBasnet/goredis/internal/pubsub"
	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/server"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

func startServer(t *testing.T) string {
	t.Helper()
	return startServerWithBuffer(t, pubsub.DefaultBufferSize)
}

func startServerWithBuffer(t *testing.T, pubsubBuffer int) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	st := store.New()
	ctx, cancel := context.WithCancel(context.Background())
	go st.RunExpiry(ctx, 10*time.Millisecond)

	cmds := command.NewRegistry(command.Config{Store: st, Broker: pubsub.NewBroker(pubsubBuffer)})
	srv := server.New(ln.Addr().String(), slog.New(slog.DiscardHandler), func(w *resp.Writer) server.Session { return cmds.NewClient(w) }, server.Config{})
	go srv.Serve(ctx, ln)
	t.Cleanup(func() {
		ln.Close()
		cancel()
	})
	return ln.Addr().String()
}

type client struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func dial(t *testing.T, addr string) *client {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	t.Cleanup(func() { conn.Close() })
	return &client{t: t, conn: conn, r: bufio.NewReader(conn)}
}

func (c *client) send(args ...string) {
	c.t.Helper()

	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := io.WriteString(c.conn, b.String()); err != nil {
		c.t.Fatal(err)
	}
}

func (c *client) do(args ...string) string {
	c.t.Helper()
	c.send(args...)
	return c.read()
}

func (c *client) read() string {
	c.t.Helper()

	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("read reply: %v", err)
	}
	line = strings.TrimSuffix(line, "\r\n")
	prefix, rest := line[0], line[1:]

	switch prefix {
	case '+':
		return rest
	case '-':
		return "(error) " + rest
	case ':':
		return "(integer) " + rest
	case '$':
		n, _ := strconv.Atoi(rest)
		if n < 0 {
			return "(nil)"
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			c.t.Fatalf("read bulk: %v", err)
		}
		return strconv.Quote(string(buf[:n]))
	case '*':
		n, _ := strconv.Atoi(rest)
		if n < 0 {
			return "(nil)"
		}
		items := make([]string, n)
		for i := range items {
			items[i] = c.read()
		}
		return "[" + strings.Join(items, " ") + "]"
	}
	c.t.Fatalf("unknown reply type %q", line)
	return ""
}
