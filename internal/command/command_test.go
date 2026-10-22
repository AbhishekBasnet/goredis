package command

import (
	"bytes"
	"testing"

	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

// step is one command and the exact RESP bytes it must reply with.
type step struct {
	args []string
	want string
}

func cmd(args ...string) []string { return args }

func TestCommands(t *testing.T) {
	tests := []struct {
		name  string
		steps []step
	}{
		{"ping", []step{
			{cmd("PING"), "+PONG\r\n"},
			{cmd("ping", "hi"), "$2\r\nhi\r\n"},
			{cmd("PING", "a", "b"), "-ERR wrong number of arguments for 'ping' command\r\n"},
		}},
		{"echo", []step{
			{cmd("ECHO", "hello world"), "$11\r\nhello world\r\n"},
			{cmd("ECHO"), "-ERR wrong number of arguments for 'echo' command\r\n"},
		}},
		{"case insensitive", []step{
			{cmd("sEt", "k", "v"), "+OK\r\n"},
			{cmd("get", "k"), "$1\r\nv\r\n"},
		}},
		{"unknown", []step{
			{cmd("FOO", "a", "b"), "-ERR unknown command 'FOO', with args beginning with: 'a' 'b' \r\n"},
			{cmd("foo"), "-ERR unknown command 'foo', with args beginning with: \r\n"},
		}},
		{"get set", []step{
			{cmd("GET", "k"), "$-1\r\n"},
			{cmd("SET", "k", "v1"), "+OK\r\n"},
			{cmd("SET", "k", "v2"), "+OK\r\n"},
			{cmd("GET", "k"), "$2\r\nv2\r\n"},
			{cmd("SET", "k", ""), "+OK\r\n"},
			{cmd("GET", "k"), "$0\r\n\r\n"},
		}},
		{"arity", []step{
			{cmd("GET"), "-ERR wrong number of arguments for 'get' command\r\n"},
			{cmd("GET", "a", "b"), "-ERR wrong number of arguments for 'get' command\r\n"},
			{cmd("SET", "k"), "-ERR wrong number of arguments for 'set' command\r\n"},
			{cmd("DEL"), "-ERR wrong number of arguments for 'del' command\r\n"},
		}},
		{"set unknown option", []step{
			{cmd("SET", "k", "v", "NX"), "-ERR syntax error\r\n"},
			{cmd("GET", "k"), "$-1\r\n"},
		}},
		{"incr decr", []step{
			{cmd("INCR", "n"), ":1\r\n"},
			{cmd("INCR", "n"), ":2\r\n"},
			{cmd("DECR", "n"), ":1\r\n"},
			{cmd("DECR", "m"), ":-1\r\n"},
			{cmd("GET", "n"), "$1\r\n1\r\n"},
		}},
		{"incr errors", []step{
			{cmd("SET", "s", "abc"), "+OK\r\n"},
			{cmd("INCR", "s"), "-ERR value is not an integer or out of range\r\n"},
			{cmd("SET", "max", "9223372036854775807"), "+OK\r\n"},
			{cmd("INCR", "max"), "-ERR increment or decrement would overflow\r\n"},
		}},
		{"mget mset", []step{
			{cmd("MSET", "a", "1", "b", "2"), "+OK\r\n"},
			{cmd("MGET", "a", "missing", "b"), "*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n2\r\n"},
			{cmd("MSET", "a", "1", "b"), "-ERR wrong number of arguments for 'mset' command\r\n"},
			{cmd("MSET", "a"), "-ERR wrong number of arguments for 'mset' command\r\n"},
		}},
		{"del exists", []step{
			{cmd("MSET", "a", "1", "b", "2"), "+OK\r\n"},
			{cmd("EXISTS", "a", "a", "b", "c"), ":3\r\n"},
			{cmd("DEL", "a", "c"), ":1\r\n"},
			{cmd("DEL", "a"), ":0\r\n"},
			{cmd("EXISTS", "a"), ":0\r\n"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry(store.New())
			for _, s := range tt.steps {
				var buf bytes.Buffer
				w := resp.NewWriter(&buf)
				r.Dispatch(w, s.args)
				w.Flush()
				if got := buf.String(); got != s.want {
					t.Errorf("%q: got %q, want %q", s.args, got, s.want)
				}
			}
		})
	}
}

func TestArityOK(t *testing.T) {
	tests := []struct {
		arity, n int
		want     bool
	}{
		{2, 2, true}, {2, 1, false}, {2, 3, false},
		{-2, 1, false}, {-2, 2, true}, {-2, 10, true},
	}
	for _, tt := range tests {
		if got := arityOK(tt.arity, tt.n); got != tt.want {
			t.Errorf("arityOK(%d, %d) = %v, want %v", tt.arity, tt.n, got, tt.want)
		}
	}
}
