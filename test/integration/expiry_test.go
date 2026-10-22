package integration

import (
	"testing"
	"time"
)

func TestExpiry(t *testing.T) {
	c := dial(t, startServer(t))

	steps := []struct {
		args []string
		want string
	}{
		{[]string{"SET", "short", "v", "PX", "50"}, "OK"},
		{[]string{"SET", "long", "v", "EX", "100"}, "OK"},
		{[]string{"SET", "kept", "v", "PX", "50"}, "OK"},
		{[]string{"PERSIST", "kept"}, "(integer) 1"},
		{[]string{"GET", "short"}, `"v"`},
		{[]string{"TTL", "long"}, "(integer) 100"},
	}
	for _, s := range steps {
		if got := c.do(s.args...); got != s.want {
			t.Errorf("%q: got %s, want %s", s.args, got, s.want)
		}
	}

	time.Sleep(100 * time.Millisecond)

	for _, s := range []struct {
		args []string
		want string
	}{
		{[]string{"GET", "short"}, "(nil)"},
		{[]string{"TTL", "short"}, "(integer) -2"},
		{[]string{"EXISTS", "long", "kept"}, "(integer) 2"},
		{[]string{"TTL", "kept"}, "(integer) -1"},
	} {
		if got := c.do(s.args...); got != s.want {
			t.Errorf("after sleep, %q: got %s, want %s", s.args, got, s.want)
		}
	}
}
