package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPubSub(t *testing.T) {
	addr := startServer(t)
	sub, pub := dial(t, addr), dial(t, addr)

	if got := sub.do("SUBSCRIBE", "news", "sport"); got != `["subscribe" "news" (integer) 1]` {
		t.Fatalf("SUBSCRIBE = %s", got)
	}
	if got := sub.read(); got != `["subscribe" "sport" (integer) 2]` {
		t.Fatalf("second confirmation = %s", got)
	}

	if got := pub.do("PUBLISH", "news", "hello"); got != "(integer) 1" {
		t.Errorf("PUBLISH = %s", got)
	}
	if got := sub.read(); got != `["message" "news" "hello"]` {
		t.Errorf("message = %s", got)
	}

	if got := sub.do("SET", "k", "v"); !strings.HasPrefix(got, "(error) ERR Can't execute 'set'") {
		t.Errorf("SET while subscribed = %s", got)
	}
	if got := sub.do("PING"); got != `["pong" ""]` {
		t.Errorf("PING while subscribed = %s", got)
	}
}

func TestPubSubFanOut(t *testing.T) {
	const subscribers, messages = 20, 50
	addr := startServer(t)

	subs := make([]*client, subscribers)
	for i := range subs {
		subs[i] = dial(t, addr)
		subs[i].do("SUBSCRIBE", "c")
	}

	pub := dial(t, addr)
	for i := range messages {
		if got, want := pub.do("PUBLISH", "c", fmt.Sprint(i)), fmt.Sprintf("(integer) %d", subscribers); got != want {
			t.Fatalf("PUBLISH %d = %s, want %s", i, got, want)
		}
	}

	for n, s := range subs {
		for i := range messages {
			if got, want := s.read(), fmt.Sprintf(`["message" "c" "%d"]`, i); got != want {
				t.Fatalf("subscriber %d, message %d: got %s, want %s", n, i, got, want)
			}
		}
	}
}

func TestSubscriberDisconnectCleansUp(t *testing.T) {
	addr := startServer(t)
	sub, pub := dial(t, addr), dial(t, addr)
	sub.do("SUBSCRIBE", "c")
	sub.conn.Close()

	deadline := time.Now().Add(time.Second)
	for pub.do("PUBLISH", "c", "x") != "(integer) 0" {
		if time.Now().After(deadline) {
			t.Fatal("subscription outlived its connection")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSlowSubscriberOverTCP(t *testing.T) {
	addr := startServerWithBuffer(t, 8)
	slow, fast, pub := dial(t, addr), dial(t, addr), dial(t, addr)
	slow.do("SUBSCRIBE", "c")
	fast.do("SUBSCRIBE", "c")

	payload := strings.Repeat("x", 64*1024)
	kicked := false
	for i := range 4000 {
		start := time.Now()
		got := pub.do("PUBLISH", "c", payload)
		if d := time.Since(start); d > time.Second {
			t.Fatalf("PUBLISH %d took %v: a slow subscriber blocked the publisher", i, d)
		}
		if got := fast.read(); !strings.HasPrefix(got, `["message" "c"`) {
			t.Fatalf("fast subscriber got %.40s", got)
		}
		if got == "(integer) 1" {
			kicked = true
			break
		}
	}
	if !kicked {
		t.Fatal("slow subscriber was never disconnected")
	}
}
