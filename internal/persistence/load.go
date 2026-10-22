package persistence

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/AbhishekBasnet/goredis/internal/resp"
)

type LoadResult struct {
	Commands       int
	TruncatedBytes int64
}

func LoadAOF(path string, apply func([]string) error) (LoadResult, error) {
	return load(path, apply, true)
}

func LoadSnapshot(path string, apply func([]string) error) (LoadResult, error) {
	return load(path, apply, false)
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func load(path string, apply func([]string) error, repairTail bool) (LoadResult, error) {
	var res LoadResult
	f, err := os.Open(path)
	if err != nil {
		return res, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return res, err
	}
	cr := &countingReader{r: f}
	r := resp.NewReader(cr)

	var good int64
	for {
		args, err := r.ReadCommand()
		switch {
		case errors.Is(err, io.EOF):
			return res, nil
		case errors.Is(err, io.ErrUnexpectedEOF) && repairTail:
			if err := os.Truncate(path, good); err != nil {
				return res, fmt.Errorf("truncate %s: %w", path, err)
			}
			res.TruncatedBytes = info.Size() - good
			return res, nil
		case err != nil:
			return res, fmt.Errorf("%s: command %d at byte %d: %w", path, res.Commands+1, good, err)
		}

		if err := apply(args); err != nil {
			return res, fmt.Errorf("%s: command %d at byte %d: %w", path, res.Commands+1, good, err)
		}
		res.Commands++
		good = cr.n - int64(r.Buffered())
	}
}
