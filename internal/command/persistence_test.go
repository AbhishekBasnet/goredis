package command

import (
	"errors"
	"testing"

	"github.com/AbhishekBasnet/goredis/internal/store"
)

type fakeSaver struct {
	saves, bgsaves int
	err            error
}

func (f *fakeSaver) Save() error           { f.saves++; return f.err }
func (f *fakeSaver) BackgroundSave() error { f.bgsaves++; return f.err }

func TestSaveCommands(t *testing.T) {
	saver := &fakeSaver{}
	c := newTestClient(NewRegistry(Config{Store: store.New(), Saver: saver}))

	if got := c.run("SAVE"); got != "+OK\r\n" {
		t.Errorf("SAVE = %q", got)
	}
	if got := c.run("BGSAVE"); got != "+Background saving started\r\n" {
		t.Errorf("BGSAVE = %q", got)
	}
	if saver.saves != 1 || saver.bgsaves != 1 {
		t.Errorf("saves = %d, bgsaves = %d", saver.saves, saver.bgsaves)
	}

	saver.err = errors.New("Background save already in progress")
	if got := c.run("BGSAVE"); got != "-ERR Background save already in progress\r\n" {
		t.Errorf("BGSAVE while saving = %q", got)
	}
	if got := c.run("SAVE", "now"); got != "-ERR wrong number of arguments for 'save' command\r\n" {
		t.Errorf("SAVE with an argument = %q", got)
	}
}

func TestSaveWithoutSnapshots(t *testing.T) {
	c := newTestClient(newTestRegistry())
	for _, name := range []string{"SAVE", "BGSAVE"} {
		if got := c.run(name); got != "-ERR snapshots are not configured\r\n" {
			t.Errorf("%s = %q", name, got)
		}
	}
}
