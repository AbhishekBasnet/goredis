package server

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/command"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

// startHandler runs handle on one end of an in-memory connection and returns
// the other end, plus a channel that is closed when the handler returns.
func startHandler(t *testing.T, st *store.Store) (net.Conn, <-chan struct{}) {
	t.Helper()

	client, srvConn := net.Pipe()
	s := New("", slog.New(slog.DiscardHandler), command.NewRegistry(st))

	done := make(chan struct{})
	go func() {
		s.handle(srvConn)
		close(done)
	}()

	client.SetDeadline(time.Now().Add(2 * time.Second))
	t.Cleanup(func() { client.Close() })
	return client, done
}

// send writes input in the background: net.Pipe is unbuffered, so a Write
// blocks until the handler reads it, and the test must be free to read replies.
func send(client net.Conn, input string) {
	go client.Write([]byte(input))
}

func TestHandle(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"ping", "*1\r\n$4\r\nPING\r\n", "+PONG\r\n"},
		{"ping lower case", "*1\r\n$4\r\nping\r\n", "+PONG\r\n"},
		{"ping with message", "*2\r\n$4\r\nPING\r\n$2\r\nhi\r\n", "$2\r\nhi\r\n"},
		{"inline ping", "PING\r\n", "+PONG\r\n"},
		{"pipelined", "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nPING\r\n$1\r\na\r\nPING\r\n",
			"+PONG\r\n$1\r\na\r\n+PONG\r\n"},
		{"wrong arity", "*3\r\n$4\r\nPING\r\n$1\r\na\r\n$1\r\nb\r\n",
			"-ERR wrong number of arguments for 'ping' command\r\n"},
		{"unknown command", "*2\r\n$3\r\nFOO\r\n$3\r\nbar\r\n",
			"-ERR unknown command 'FOO', with args beginning with: 'bar' \r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, done := startHandler(t, store.New())
			send(client, tt.input)

			got := make([]byte, len(tt.want))
			if _, err := io.ReadFull(client, got); err != nil {
				t.Fatalf("read reply: %v (got %q)", err, got)
			}
			if string(got) != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}

			client.Close()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("handler did not exit after the client disconnected")
			}
		})
	}
}

// After a protocol error the server replies once and closes the connection
// itself; the PING after the bad header must never be answered.
func TestHandleProtocolError(t *testing.T) {
	client, done := startHandler(t, store.New())
	send(client, "*abc\r\nPING\r\n")

	got, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if want := "-ERR Protocol error: invalid multibulk length\r\n"; string(got) != want {
		t.Errorf("got %q, want %q", got, want)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not exit after a protocol error")
	}
}

// readExactly reads len(want) bytes and compares them with want.
func readExactly(t *testing.T, client net.Conn, want string) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("read: %v (got %q, want %q)", err, got, want)
	}
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// A reply to an earlier pipelined command must not wait in the buffer while
// BLPOP blocks, and the connection must work normally once BLPOP returns.
func TestBlockingFlushesFirst(t *testing.T) {
	st := store.New()
	client, _ := startHandler(t, st)
	send(client, "PING\r\nBLPOP k 0\r\n")

	// Without the flush before blocking, this read would time out.
	readExactly(t, client, "+PONG\r\n")

	st.Push("k", false, "v")
	readExactly(t, client, "*2\r\n$1\r\nk\r\n$1\r\nv\r\n")

	// The watcher's read deadline must have been cleared.
	send(client, "PING\r\n")
	readExactly(t, client, "+PONG\r\n")
}

// A client that hangs up during BLPOP must leave the queue straight away.
// Otherwise the next push hands it the element, and the element is lost.
func TestBlockedClientHangsUp(t *testing.T) {
	st := store.New()
	client, done := startHandler(t, st)
	send(client, "PING\r\nBLPOP k 0\r\n")
	readExactly(t, client, "+PONG\r\n") // BLPOP is now running

	client.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler stayed blocked after the client hung up")
	}

	st.Push("k", false, "v")
	if n, _ := st.LLen("k"); n != 1 {
		t.Errorf("LLEN = %d, want 1: the element went to the departed client", n)
	}
}
