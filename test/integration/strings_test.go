package integration

import (
	"strconv"
	"sync"
	"testing"
)

func TestStringCommands(t *testing.T) {
	c := dial(t, startServer(t))

	steps := []struct {
		args []string
		want string
	}{
		{[]string{"PING"}, "PONG"},
		{[]string{"ECHO", "hi there"}, `"hi there"`},
		{[]string{"GET", "k"}, "(nil)"},
		{[]string{"SET", "k", "v"}, "OK"},
		{[]string{"GET", "k"}, `"v"`},
		{[]string{"INCR", "n"}, "(integer) 1"},
		{[]string{"DECR", "n"}, "(integer) 0"},
		{[]string{"INCR", "k"}, "(error) ERR value is not an integer or out of range"},
		{[]string{"MSET", "a", "1", "b", "2"}, "OK"},
		{[]string{"MGET", "a", "nope", "b"}, `["1" (nil) "2"]`},
		{[]string{"EXISTS", "a", "b", "nope"}, "(integer) 2"},
		{[]string{"DEL", "a", "b", "nope"}, "(integer) 2"},
		{[]string{"get"}, "(error) ERR wrong number of arguments for 'get' command"},
		{[]string{"NOPE"}, "(error) ERR unknown command 'NOPE', with args beginning with: "},
	}
	for _, s := range steps {
		if got := c.do(s.args...); got != s.want {
			t.Errorf("%q: got %s, want %s", s.args, got, s.want)
		}
	}
}

// Send many commands before reading any reply; they must come back in order.
func TestPipelining(t *testing.T) {
	c := dial(t, startServer(t))

	const n = 1000
	for range n {
		c.send("INCR", "p")
	}
	for i := 1; i <= n; i++ {
		if got, want := c.read(), "(integer) "+strconv.Itoa(i); got != want {
			t.Fatalf("reply %d: got %s, want %s", i, got, want)
		}
	}
}

// 100 clients INCR the same key at once. Every increment must land exactly
// once, which only holds if IncrBy is atomic under the store's lock.
func TestConcurrentIncr(t *testing.T) {
	const clients, perClient = 100, 50
	addr := startServer(t)

	var wg sync.WaitGroup
	for range clients {
		c := dial(t, addr)
		wg.Go(func() {
			for range perClient {
				c.do("INCR", "counter")
			}
		})
	}
	wg.Wait()

	got := dial(t, addr).do("GET", "counter")
	if want := strconv.Quote(strconv.Itoa(clients * perClient)); got != want {
		t.Errorf("counter = %s, want %s", got, want)
	}
}

// Keys written by one connection are visible to another: there is one
// shared store, not one per client.
func TestSharedStore(t *testing.T) {
	addr := startServer(t)
	a, b := dial(t, addr), dial(t, addr)

	a.do("SET", "shared", "yes")
	if got := b.do("GET", "shared"); got != `"yes"` {
		t.Errorf("second client got %s", got)
	}
}
