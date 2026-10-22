// Package integration starts a real goredis server on a random port and
// drives it over TCP with raw RESP, exactly as redis-cli would.
package integration

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/command"
	"github.com/AbhishekBasnet/goredis/internal/server"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

// startServer runs a fresh server with an empty store and returns its address.
func startServer(t *testing.T) string {
	t.Helper()

	// Port 0: the OS picks a free port, so tests never clash.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(ln.Addr().String(), slog.New(slog.DiscardHandler), command.NewRegistry(store.New()))
	go srv.Serve(ln)
	t.Cleanup(func() { ln.Close() })
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

// send writes one command as an array of bulk strings without waiting for
// the reply. Call it several times and then read() to pipeline.
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

// do sends one command and returns its reply.
func (c *client) do(args ...string) string {
	c.t.Helper()
	c.send(args...)
	return c.read()
}

// read returns one reply, formatted the way redis-cli prints it:
// OK, "value", (nil), (integer) 3, (error) ERR ..., [a b] for arrays.
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
