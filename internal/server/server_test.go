package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/command"
	"github.com/AbhishekBasnet/goredis/internal/pubsub"
	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

func newRegistry(st *store.Store) *command.Registry {
	return command.NewRegistry(command.Config{Store: st, Broker: pubsub.NewBroker(16)})
}

func startHandler(t *testing.T, reg *command.Registry) (net.Conn, <-chan struct{}) {
	t.Helper()

	client, srvConn := net.Pipe()
	s := New("", slog.New(slog.DiscardHandler), func(w *resp.Writer) Session { return reg.NewClient(w) }, Config{})

	done := make(chan struct{})
	go func() {
		s.handle(context.Background(), srvConn)
		close(done)
	}()

	client.SetDeadline(time.Now().Add(2 * time.Second))
	t.Cleanup(func() { client.Close() })
	return client, done
}

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
			client, done := startHandler(t, newRegistry(store.New()))
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

func TestHandleProtocolError(t *testing.T) {
	client, done := startHandler(t, newRegistry(store.New()))
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

func TestBlockingFlushesFirst(t *testing.T) {
	st := store.New()
	client, _ := startHandler(t, newRegistry(st))
	send(client, "PING\r\nBLPOP k 0\r\n")

	readExactly(t, client, "+PONG\r\n")

	st.Push("k", false, "v")
	readExactly(t, client, "*2\r\n$1\r\nk\r\n$1\r\nv\r\n")

	send(client, "PING\r\n")
	readExactly(t, client, "+PONG\r\n")
}

func TestBlockedClientHangsUp(t *testing.T) {
	st := store.New()
	client, done := startHandler(t, newRegistry(st))
	send(client, "PING\r\nBLPOP k 0\r\n")
	readExactly(t, client, "+PONG\r\n")

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

func TestPubSubOverConnections(t *testing.T) {
	reg := newRegistry(store.New())
	sub, _ := startHandler(t, reg)
	pub, _ := startHandler(t, reg)

	send(sub, "SUBSCRIBE news\r\n")
	readExactly(t, sub, "*3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n")

	send(pub, "PUBLISH news hello\r\nPUBLISH news world\r\n")
	readExactly(t, pub, ":1\r\n:1\r\n")
	readExactly(t, sub, "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nworld\r\n")

	send(sub, "GET k\r\nUNSUBSCRIBE\r\nGET k\r\n")
	readExactly(t, sub, "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"+
		"*3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:0\r\n"+
		"$-1\r\n")
}

func TestSubscriberHangsUp(t *testing.T) {
	reg := newRegistry(store.New())
	sub, subDone := startHandler(t, reg)
	pub, _ := startHandler(t, reg)

	send(sub, "SUBSCRIBE a b\r\n")
	readExactly(t, sub, "*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n")
	sub.Close()
	<-subDone

	send(pub, "PUBLISH a x\r\n")
	readExactly(t, pub, ":0\r\n")
}

func TestSlowSubscriberDisconnected(t *testing.T) {
	reg := command.NewRegistry(command.Config{Store: store.New(), Broker: pubsub.NewBroker(2)})
	sub, subDone := startHandler(t, reg)
	pub, _ := startHandler(t, reg)

	send(sub, "SUBSCRIBE c\r\n")
	readExactly(t, sub, "*3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n")

	start := time.Now()
	for range 10 {
		send(pub, "PUBLISH c x\r\n")
		reply := make([]byte, 4)
		if _, err := io.ReadFull(pub, reply); err != nil {
			t.Fatalf("publisher blocked: %v", err)
		}
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("10 publishes took %v: the slow subscriber held them up", d)
	}

	select {
	case <-subDone:
	case <-time.After(time.Second):
		t.Fatal("slow subscriber was not disconnected")
	}
	send(pub, "PUBLISH c x\r\n")
	readExactly(t, pub, ":0\r\n")
}
