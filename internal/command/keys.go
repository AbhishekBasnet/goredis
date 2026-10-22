package command

import (
	"context"
	"time"

	"github.com/AbhishekBasnet/goredis/internal/store"
)

func del(_ context.Context, c *Client, args []string) {
	c.w.WriteInteger(int64(c.store.Del(args[1:]...)))
}

func exists(_ context.Context, c *Client, args []string) {
	c.w.WriteInteger(int64(c.store.Exists(args[1:]...)))
}

func expire(_ context.Context, c *Client, args []string) {
	ttl, err := parseTTL(args[2], time.Second, "expire")
	if err != nil {
		c.w.WriteError(err.Error())
		return
	}
	c.w.WriteInteger(boolInt(c.store.Expire(args[1], ttl)))
}

func ttl(_ context.Context, c *Client, args []string) {
	c.writeTTL(args[1], time.Second)
}

func pttl(_ context.Context, c *Client, args []string) {
	c.writeTTL(args[1], time.Millisecond)
}

func (c *Client) writeTTL(key string, unit time.Duration) {
	d, ok := c.store.TTL(key)
	switch {
	case !ok:
		c.w.WriteInteger(-2)
	case d == store.NoExpiry:
		c.w.WriteInteger(-1)
	default:
		c.w.WriteInteger(int64(d.Round(unit) / unit))
	}
}

func persist(_ context.Context, c *Client, args []string) {
	c.w.WriteInteger(boolInt(c.store.Persist(args[1])))
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
