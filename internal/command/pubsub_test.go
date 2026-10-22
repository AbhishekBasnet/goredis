package command

import (
	"testing"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/pubsub"
)

func TestSubscribeReplies(t *testing.T) {
	c := newTestClient(newTestRegistry())

	steps := []step{
		{cmd("UNSUBSCRIBE"), "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"},
		{cmd("SUBSCRIBE", "a", "b"), "*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n"},
		{cmd("SUBSCRIBE", "a"), "*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:2\r\n"},
		{cmd("PING"), "*2\r\n$4\r\npong\r\n$0\r\n\r\n"},
		{cmd("PING", "hi"), "*2\r\n$4\r\npong\r\n$2\r\nhi\r\n"},
		{cmd("GET", "k"), "-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n"},
		{cmd("NOPE"), "-ERR unknown command 'NOPE', with args beginning with: \r\n"},
		{cmd("UNSUBSCRIBE", "a"), "*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n"},
		{cmd("UNSUBSCRIBE", "zz"), "*3\r\n$11\r\nunsubscribe\r\n$2\r\nzz\r\n:1\r\n"},
		{cmd("UNSUBSCRIBE"), "*3\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:0\r\n"},
		{cmd("PING"), "+PONG\r\n"},
		{cmd("GET", "k"), "$-1\r\n"},
		{cmd("SUBSCRIBE"), "-ERR wrong number of arguments for 'subscribe' command\r\n"},
	}
	for _, s := range steps {
		if got := c.run(s.args...); got != s.want {
			t.Errorf("%q: got %q, want %q", s.args, got, s.want)
		}
	}
}

func TestPublishReachesSubscribers(t *testing.T) {
	r := newTestRegistry()
	sub1, sub2, pub := newTestClient(r), newTestClient(r), newTestClient(r)
	sub1.run("SUBSCRIBE", "news")
	sub2.run("SUBSCRIBE", "news", "sport")

	if got := pub.run("PUBLISH", "news", "hello"); got != ":2\r\n" {
		t.Errorf("PUBLISH news = %q", got)
	}
	if got := pub.run("PUBLISH", "nobody", "x"); got != ":0\r\n" {
		t.Errorf("PUBLISH nobody = %q", got)
	}

	for name, c := range map[string]*testClient{"sub1": sub1, "sub2": sub2} {
		select {
		case m := <-c.Subscriber().Messages():
			if m != (pubsub.Message{Channel: "news", Payload: "hello"}) {
				t.Errorf("%s got %+v", name, m)
			}
		case <-time.After(time.Second):
			t.Errorf("%s got nothing", name)
		}
	}
}

func TestCloseUnsubscribes(t *testing.T) {
	r := newTestRegistry()
	sub, pub := newTestClient(r), newTestClient(r)
	sub.run("SUBSCRIBE", "a", "b")
	sub.Close()

	if got := pub.run("PUBLISH", "a", "x"); got != ":0\r\n" {
		t.Errorf("PUBLISH after Close = %q, want :0", got)
	}
}
