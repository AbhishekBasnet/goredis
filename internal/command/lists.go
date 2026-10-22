package command

import (
	"context"
	"fmt"
	"strings"
)

func lpush(_ context.Context, c *Client, args []string) {
	c.push(args, true)
}

func rpush(_ context.Context, c *Client, args []string) {
	c.push(args, false)
}

func (c *Client) push(args []string, front bool) {
	n, err := c.store.Push(args[1], front, args[2:]...)
	if err != nil {
		c.writeStoreError(err)
		return
	}
	c.w.WriteInteger(int64(n))
}

func lpop(_ context.Context, c *Client, args []string) {
	c.pop(args, true)
}

func rpop(_ context.Context, c *Client, args []string) {
	c.pop(args, false)
}

func (c *Client) pop(args []string, front bool) {
	if len(args) > 3 {
		c.w.WriteError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(args[0])))
		return
	}

	count, withCount := 1, len(args) == 3
	if withCount {
		n, err := parseInt(args[2])
		if err != nil {
			c.w.WriteError(err.Error())
			return
		}
		if n < 0 {
			c.w.WriteError("ERR value is out of range, must be positive")
			return
		}
		count = int(n)
	}

	values, exists, err := c.store.Pop(args[1], front, count)
	switch {
	case err != nil:
		c.writeStoreError(err)
	case !withCount && !exists:
		c.w.WriteNull()
	case !withCount:
		c.w.WriteBulk(values[0])
	case !exists:
		c.w.WriteNullArray()
	default:
		c.writeBulks(values...)
	}
}

func llen(_ context.Context, c *Client, args []string) {
	n, err := c.store.LLen(args[1])
	if err != nil {
		c.writeStoreError(err)
		return
	}
	c.w.WriteInteger(int64(n))
}

func lrange(_ context.Context, c *Client, args []string) {
	start, err1 := parseInt(args[2])
	stop, err2 := parseInt(args[3])
	if err1 != nil || err2 != nil {
		c.w.WriteError(errNotInteger.Error())
		return
	}

	values, err := c.store.LRange(args[1], int(start), int(stop))
	if err != nil {
		c.writeStoreError(err)
		return
	}
	c.writeBulks(values...)
}

func blpop(ctx context.Context, c *Client, args []string) {
	c.bpop(ctx, args, true)
}

func brpop(ctx context.Context, c *Client, args []string) {
	c.bpop(ctx, args, false)
}

func (c *Client) bpop(ctx context.Context, args []string, front bool) {
	timeout, err := parseTimeout(args[len(args)-1])
	if err != nil {
		c.w.WriteError(err.Error())
		return
	}

	p, ok, err := c.store.BPop(ctx, args[1:len(args)-1], front, timeout)
	switch {
	case err != nil:
		c.writeStoreError(err)
	case !ok:
		c.w.WriteNullArray()
	default:
		c.writeBulks(p.Key, p.Value)
	}
}

func (c *Client) writeBulks(values ...string) {
	c.w.WriteArray(len(values))
	for _, v := range values {
		c.w.WriteBulk(v)
	}
}
