package command

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/AbhishekBasnet/goredis/internal/pubsub"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

type Handler func(ctx context.Context, c *Client, args []string)

type Command struct {
	Name     string
	Arity    int
	Handler  Handler
	Blocking bool
	PubSub   bool
}

type Registry struct {
	store  *store.Store
	broker *pubsub.Broker
	cmds   map[string]Command
}

func NewRegistry(st *store.Store, broker *pubsub.Broker) *Registry {
	r := &Registry{store: st, broker: broker, cmds: make(map[string]Command)}
	for _, c := range []Command{
		{Name: "ping", Arity: -1, Handler: ping, PubSub: true},
		{Name: "echo", Arity: 2, Handler: echo},

		{Name: "get", Arity: 2, Handler: get},
		{Name: "set", Arity: -3, Handler: set},
		{Name: "incr", Arity: 2, Handler: incr},
		{Name: "decr", Arity: 2, Handler: decr},
		{Name: "mget", Arity: -2, Handler: mget},
		{Name: "mset", Arity: -3, Handler: mset},

		{Name: "del", Arity: -2, Handler: del},
		{Name: "exists", Arity: -2, Handler: exists},
		{Name: "expire", Arity: 3, Handler: expire},
		{Name: "ttl", Arity: 2, Handler: ttl},
		{Name: "pttl", Arity: 2, Handler: pttl},
		{Name: "persist", Arity: 2, Handler: persist},

		{Name: "lpush", Arity: -3, Handler: lpush},
		{Name: "rpush", Arity: -3, Handler: rpush},
		{Name: "lpop", Arity: -2, Handler: lpop},
		{Name: "rpop", Arity: -2, Handler: rpop},
		{Name: "llen", Arity: 2, Handler: llen},
		{Name: "lrange", Arity: 4, Handler: lrange},
		{Name: "blpop", Arity: -3, Handler: blpop, Blocking: true},
		{Name: "brpop", Arity: -3, Handler: brpop, Blocking: true},

		{Name: "subscribe", Arity: -2, Handler: subscribe, PubSub: true},
		{Name: "unsubscribe", Arity: -1, Handler: unsubscribe, PubSub: true},
		{Name: "publish", Arity: 3, Handler: publish},
	} {
		r.cmds[c.Name] = c
	}
	return r
}

func arityOK(arity, n int) bool {
	if arity >= 0 {
		return n == arity
	}
	return n >= -arity
}

func unknownCommand(args []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ERR unknown command '%s', with args beginning with: ", args[0])
	for _, a := range args[1:] {
		fmt.Fprintf(&b, "'%s' ", a)
	}
	return b.String()
}

func (c *Client) writeStoreError(err error) {
	code := "ERR "
	if errors.Is(err, store.ErrWrongType) {
		code = "WRONGTYPE "
	}
	c.w.WriteError(code + err.Error())
}
