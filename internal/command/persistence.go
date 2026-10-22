package command

import "context"

type Saver interface {
	Save() error
	BackgroundSave() error
}

func save(_ context.Context, c *Client, args []string) {
	if c.saver == nil {
		c.w.WriteError("ERR snapshots are not configured")
		return
	}
	if err := c.saver.Save(); err != nil {
		c.w.WriteError("ERR " + err.Error())
		return
	}
	c.w.WriteSimple("OK")
}

func bgsave(_ context.Context, c *Client, args []string) {
	if c.saver == nil {
		c.w.WriteError("ERR snapshots are not configured")
		return
	}
	if err := c.saver.BackgroundSave(); err != nil {
		c.w.WriteError("ERR " + err.Error())
		return
	}
	c.w.WriteSimple("Background saving started")
}
