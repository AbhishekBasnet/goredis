package command

import "context"

func ping(_ context.Context, c *Client, args []string) {
	if len(args) > 2 {
		c.w.WriteError("ERR wrong number of arguments for 'ping' command")
		return
	}
	if c.subscribed() {
		c.w.WriteArray(2)
		c.w.WriteBulk("pong")
		if len(args) == 2 {
			c.w.WriteBulk(args[1])
		} else {
			c.w.WriteBulk("")
		}
		return
	}
	if len(args) == 2 {
		c.w.WriteBulk(args[1])
		return
	}
	c.w.WriteSimple("PONG")
}

func echo(_ context.Context, c *Client, args []string) {
	c.w.WriteBulk(args[1])
}
