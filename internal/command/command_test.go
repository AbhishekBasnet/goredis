package command

import (
	"bytes"
	"context"
	"strconv"
	"strings"
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
		{"set options", []step{
			{cmd("SET", "k", "v", "NX"), "-ERR syntax error\r\n"},
			{cmd("SET", "k", "v", "EX"), "-ERR syntax error\r\n"},
			{cmd("SET", "k", "v", "EX", "10", "PX", "100"), "-ERR syntax error\r\n"},
			{cmd("SET", "k", "v", "EX", "ten"), "-ERR value is not an integer or out of range\r\n"},
			{cmd("SET", "k", "v", "EX", "0"), "-ERR invalid expire time in 'set' command\r\n"},
			{cmd("SET", "k", "v", "PX", "-5"), "-ERR invalid expire time in 'set' command\r\n"},
			{cmd("SET", "k", "v", "EX", "9223372036854775807"), "-ERR invalid expire time in 'set' command\r\n"},
			{cmd("GET", "k"), "$-1\r\n"},
			{cmd("SET", "k", "v", "ex", "100"), "+OK\r\n"},
			{cmd("TTL", "k"), ":100\r\n"},
			{cmd("SET", "k", "v", "PX", "100000"), "+OK\r\n"},
			{cmd("TTL", "k"), ":100\r\n"},
			{cmd("SET", "k", "v"), "+OK\r\n"},
			{cmd("TTL", "k"), ":-1\r\n"},
		}},
		{"expire ttl persist", []step{
			{cmd("TTL", "k"), ":-2\r\n"},
			{cmd("PTTL", "k"), ":-2\r\n"},
			{cmd("EXPIRE", "k", "10"), ":0\r\n"},
			{cmd("SET", "k", "v"), "+OK\r\n"},
			{cmd("TTL", "k"), ":-1\r\n"},
			{cmd("PTTL", "k"), ":-1\r\n"},
			{cmd("PERSIST", "k"), ":0\r\n"},
			{cmd("EXPIRE", "k", "50"), ":1\r\n"},
			{cmd("TTL", "k"), ":50\r\n"},
			{cmd("PERSIST", "k"), ":1\r\n"},
			{cmd("TTL", "k"), ":-1\r\n"},
			{cmd("EXPIRE", "k", "abc"), "-ERR value is not an integer or out of range\r\n"},
			{cmd("EXPIRE", "k"), "-ERR wrong number of arguments for 'expire' command\r\n"},
			{cmd("EXPIRE", "k", "-1"), ":1\r\n"},
			{cmd("EXISTS", "k"), ":0\r\n"},
		}},
		{"incr keeps ttl", []step{
			{cmd("SET", "n", "1", "EX", "100"), "+OK\r\n"},
			{cmd("INCR", "n"), ":2\r\n"},
			{cmd("TTL", "n"), ":100\r\n"},
		}},
		{"lists", []step{
			{cmd("LPUSH", "l", "a", "b"), ":2\r\n"},
			{cmd("RPUSH", "l", "c"), ":3\r\n"},
			{cmd("LRANGE", "l", "0", "-1"), "*3\r\n$1\r\nb\r\n$1\r\na\r\n$1\r\nc\r\n"},
			{cmd("LRANGE", "l", "5", "10"), "*0\r\n"},
			{cmd("LRANGE", "nope", "0", "-1"), "*0\r\n"},
			{cmd("LRANGE", "l", "x", "1"), "-ERR value is not an integer or out of range\r\n"},
			{cmd("LLEN", "l"), ":3\r\n"},
			{cmd("LLEN", "nope"), ":0\r\n"},
			{cmd("LPOP", "l"), "$1\r\nb\r\n"},
			{cmd("RPOP", "l"), "$1\r\nc\r\n"},
			{cmd("RPOP", "l"), "$1\r\na\r\n"},
			{cmd("EXISTS", "l"), ":0\r\n"},
			{cmd("LPOP", "l"), "$-1\r\n"},
		}},
		{"pop count", []step{
			{cmd("RPUSH", "l", "a", "b", "c"), ":3\r\n"},
			{cmd("LPOP", "l", "2"), "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
			{cmd("LPOP", "l", "0"), "*0\r\n"},
			{cmd("RPOP", "l", "5"), "*1\r\n$1\r\nc\r\n"},
			{cmd("LPOP", "l", "1"), "*-1\r\n"},
			{cmd("LPOP", "l", "-1"), "-ERR value is out of range, must be positive\r\n"},
			{cmd("LPOP", "l", "x"), "-ERR value is not an integer or out of range\r\n"},
			{cmd("LPOP", "l", "1", "2"), "-ERR wrong number of arguments for 'lpop' command\r\n"},
		}},
		{"wrongtype", []step{
			{cmd("SET", "s", "v"), "+OK\r\n"},
			{cmd("RPUSH", "l", "a"), ":1\r\n"},
			{cmd("LPUSH", "s", "x"), "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
			{cmd("LLEN", "s"), "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
			{cmd("BLPOP", "s", "0"), "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
			{cmd("GET", "l"), "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
			{cmd("INCR", "l"), "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
			{cmd("MGET", "s", "l"), "*2\r\n$1\r\nv\r\n$-1\r\n"},
			{cmd("SET", "l", "now a string"), "+OK\r\n"},
			{cmd("GET", "l"), "$12\r\nnow a string\r\n"},
		}},
		{"blpop without waiting", []step{
			{cmd("RPUSH", "b", "1", "2"), ":2\r\n"},
			{cmd("BLPOP", "a", "b", "0"), "*2\r\n$1\r\nb\r\n$1\r\n1\r\n"},
			{cmd("BRPOP", "b", "0"), "*2\r\n$1\r\nb\r\n$1\r\n2\r\n"},
			{cmd("BLPOP", "b", "0.01"), "*-1\r\n"},
			{cmd("BLPOP", "b", "-1"), "-ERR timeout is negative\r\n"},
			{cmd("BLPOP", "b", "soon"), "-ERR timeout is not a float or out of range\r\n"},
			{cmd("BLPOP", "b", "inf"), "-ERR timeout is not a float or out of range\r\n"},
			{cmd("BLPOP", "b"), "-ERR wrong number of arguments for 'blpop' command\r\n"},
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
				r.Dispatch(context.Background(), w, s.args)
				w.Flush()
				if got := buf.String(); got != s.want {
					t.Errorf("%q: got %q, want %q", s.args, got, s.want)
				}
			}
		})
	}
}

// PTTL is in milliseconds, so the exact value depends on how long the test
// took; check that it is in range rather than equal.
func TestPTTL(t *testing.T) {
	r := NewRegistry(store.New())
	run := func(args ...string) string {
		var buf bytes.Buffer
		w := resp.NewWriter(&buf)
		r.Dispatch(context.Background(), w, args)
		w.Flush()
		return buf.String()
	}

	run("SET", "k", "v", "PX", "5000")
	got := strings.TrimSuffix(strings.TrimPrefix(run("PTTL", "k"), ":"), "\r\n")
	ms, err := strconv.Atoi(got)
	if err != nil || ms <= 4000 || ms > 5000 {
		t.Errorf("PTTL = %q, want a value in (4000, 5000]", got)
	}
}

func TestBlocks(t *testing.T) {
	r := NewRegistry(store.New())
	for name, want := range map[string]bool{"BLPOP": true, "brpop": true, "LPOP": false, "nope": false} {
		if got := r.Blocks([]string{name}); got != want {
			t.Errorf("Blocks(%s) = %v, want %v", name, got, want)
		}
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
