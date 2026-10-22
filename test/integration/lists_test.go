package integration

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestListCommands(t *testing.T) {
	c := dial(t, startServer(t))

	steps := []struct {
		args []string
		want string
	}{
		{[]string{"RPUSH", "l", "a", "b", "c"}, "(integer) 3"},
		{[]string{"LPUSH", "l", "z"}, "(integer) 4"},
		{[]string{"LRANGE", "l", "0", "-1"}, `["z" "a" "b" "c"]`},
		{[]string{"LPOP", "l"}, `"z"`},
		{[]string{"RPOP", "l", "2"}, `["c" "b"]`},
		{[]string{"LLEN", "l"}, "(integer) 1"},
		{[]string{"SET", "s", "v"}, "OK"},
		{[]string{"LPUSH", "s", "x"}, "(error) WRONGTYPE Operation against a key holding the wrong kind of value"},
		{[]string{"GET", "l"}, "(error) WRONGTYPE Operation against a key holding the wrong kind of value"},
	}
	for _, s := range steps {
		if got := c.do(s.args...); got != s.want {
			t.Errorf("%q: got %s, want %s", s.args, got, s.want)
		}
	}
}

func TestBlockingPop(t *testing.T) {
	addr := startServer(t)
	waiting, pusher := dial(t, addr), dial(t, addr)

	waiting.send("BLPOP", "nothing", "jobs", "0")
	time.Sleep(50 * time.Millisecond)

	if got := pusher.do("RPUSH", "jobs", "job-1"); got != "(integer) 1" {
		t.Errorf("RPUSH = %s", got)
	}
	if got := waiting.read(); got != `["jobs" "job-1"]` {
		t.Errorf("BLPOP = %s", got)
	}
	if got := pusher.do("LLEN", "jobs"); got != "(integer) 0" {
		t.Errorf("LLEN = %s", got)
	}
}

func TestBlockingPopTimeout(t *testing.T) {
	c := dial(t, startServer(t))

	start := time.Now()
	if got := c.do("BLPOP", "nothing", "0.1"); got != "(nil)" {
		t.Errorf("BLPOP = %s, want (nil)", got)
	}
	if d := time.Since(start); d < 100*time.Millisecond {
		t.Errorf("BLPOP returned after %v, before its timeout", d)
	}
}

func TestProducersConsumersOverTCP(t *testing.T) {
	const producers, consumers, perProducer = 8, 8, 200
	addr := startServer(t)

	var mu sync.Mutex
	seen := make(map[string]int)

	var wg sync.WaitGroup
	for range consumers {
		c := dial(t, addr)
		wg.Go(func() {
			for {
				reply := c.do("BLPOP", "queue", "5")
				if reply == "(nil)" {
					t.Error("consumer timed out: an element was lost")
					return
				}
				value := strings.TrimSuffix(strings.TrimPrefix(reply, `["queue" "`), `"]`)
				if value == "stop" {
					return
				}
				mu.Lock()
				seen[value]++
				mu.Unlock()
			}
		})
	}

	var producing sync.WaitGroup
	for i := range producers {
		c := dial(t, addr)
		producing.Go(func() {
			for j := range perProducer {
				c.do("RPUSH", "queue", fmt.Sprintf("job-%d-%d", i, j))
			}
		})
	}
	producing.Wait()

	stopper := dial(t, addr)
	for range consumers {
		stopper.do("RPUSH", "queue", "stop")
	}
	wg.Wait()

	if len(seen) != producers*perProducer {
		t.Errorf("%d distinct jobs delivered, want %d", len(seen), producers*perProducer)
	}
	for job, n := range seen {
		if n != 1 {
			t.Errorf("%s delivered %d times", job, n)
		}
	}
}
