package command

import (
	"context"
	"strings"
	"time"
)

const errSyntax = "ERR syntax error"

func get(_ context.Context, c *Client, args []string) {
	v, ok, err := c.store.Get(args[1])
	switch {
	case err != nil:
		c.writeStoreError(err)
	case !ok:
		c.w.WriteNull()
	default:
		c.w.WriteBulk(v)
	}
}

func set(_ context.Context, c *Client, args []string) {
	var expiry time.Duration
	for i := 3; i < len(args); i += 2 {
		var unit time.Duration
		switch strings.ToUpper(args[i]) {
		case "EX":
			unit = time.Second
		case "PX":
			unit = time.Millisecond
		}
		if unit == 0 || expiry != 0 || i+1 == len(args) {
			c.w.WriteError(errSyntax)
			return
		}

		var err error
		if expiry, err = parseTTL(args[i+1], unit, "set"); err != nil {
			c.w.WriteError(err.Error())
			return
		}
		if expiry <= 0 {
			c.w.WriteError(invalidExpire("set"))
			return
		}
	}
	c.store.Set(args[1], args[2], expiry)
	c.w.WriteSimple("OK")
}

func incr(_ context.Context, c *Client, args []string) {
	c.incrBy(args[1], 1)
}

func decr(_ context.Context, c *Client, args []string) {
	c.incrBy(args[1], -1)
}

func (c *Client) incrBy(key string, delta int64) {
	n, err := c.store.IncrBy(key, delta)
	if err != nil {
		c.writeStoreError(err)
		return
	}
	c.w.WriteInteger(n)
}

func mget(_ context.Context, c *Client, args []string) {
	values, found := c.store.MGet(args[1:]...)
	c.w.WriteArray(len(values))
	for i, v := range values {
		if found[i] {
			c.w.WriteBulk(v)
		} else {
			c.w.WriteNull()
		}
	}
}

func mset(_ context.Context, c *Client, args []string) {
	if len(args)%2 == 0 {
		c.w.WriteError("ERR wrong number of arguments for 'mset' command")
		return
	}
	c.store.MSet(args[1:]...)
	c.w.WriteSimple("OK")
}
