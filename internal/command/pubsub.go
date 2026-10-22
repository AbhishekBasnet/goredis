package command

import "context"

func subscribe(_ context.Context, c *Client, args []string) {
	if c.sub == nil {
		c.sub = c.broker.NewSubscriber()
	}
	for _, channel := range args[1:] {
		c.writeSubscription("subscribe", channel, c.sub.Subscribe(channel))
	}
}

func unsubscribe(_ context.Context, c *Client, args []string) {
	channels := args[1:]
	if len(channels) == 0 && c.sub != nil {
		channels = c.sub.Channels()
	}
	if len(channels) == 0 {
		c.w.WriteArray(3)
		c.w.WriteBulk("unsubscribe")
		c.w.WriteNull()
		c.w.WriteInteger(0)
		return
	}
	for _, channel := range channels {
		n := 0
		if c.sub != nil {
			n = c.sub.Unsubscribe(channel)
		}
		c.writeSubscription("unsubscribe", channel, n)
	}
}

func publish(_ context.Context, c *Client, args []string) {
	c.w.WriteInteger(int64(c.broker.Publish(args[1], args[2])))
}

func (c *Client) writeSubscription(kind, channel string, count int) {
	c.w.WriteArray(3)
	c.w.WriteBulk(kind)
	c.w.WriteBulk(channel)
	c.w.WriteInteger(int64(count))
}
