package command

import (
	"context"
	"fmt"
	"strings"

	"github.com/AbhishekBasnet/goredis/internal/pubsub"
	"github.com/AbhishekBasnet/goredis/internal/resp"
	"github.com/AbhishekBasnet/goredis/internal/store"
)

type Client struct {
	reg    *Registry
	w      *resp.Writer
	store  *store.Store
	broker *pubsub.Broker
	saver  Saver
	sub    *pubsub.Subscriber
}

func (r *Registry) NewClient(w *resp.Writer) *Client {
	return &Client{reg: r, w: w, store: r.store, broker: r.broker, saver: r.saver}
}

func (c *Client) Dispatch(ctx context.Context, args []string) {
	cmd, ok := c.reg.cmds[strings.ToLower(args[0])]
	if !ok {
		c.w.WriteError(unknownCommand(args))
		return
	}
	if !arityOK(cmd.Arity, len(args)) {
		c.w.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd.Name))
		return
	}
	if c.subscribed() && !cmd.PubSub {
		c.w.WriteError(fmt.Sprintf("ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", cmd.Name))
		return
	}
	cmd.Handler(ctx, c, args)
}

func (c *Client) Blocks(args []string) bool {
	return c.reg.cmds[strings.ToLower(args[0])].Blocking
}

func (c *Client) Subscriber() *pubsub.Subscriber {
	return c.sub
}

func (c *Client) Close() {
	if c.sub != nil {
		c.sub.Close()
	}
}

func (c *Client) subscribed() bool {
	return c.sub != nil && c.sub.Count() > 0
}
