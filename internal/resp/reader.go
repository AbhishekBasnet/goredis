package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var ErrProtocol = errors.New("Protocol error")

const (
	maxArrayLen = 1024 * 1024
	maxBulkLen  = 512 * 1024 * 1024
)

type Reader struct {
	rd *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{rd: bufio.NewReader(r)}
}

func (r *Reader) Buffered() int {
	return r.rd.Buffered()
}

func (r *Reader) WaitReadable() error {
	_, err := r.rd.Peek(1)
	return err
}

func (r *Reader) ReadCommand() ([]string, error) {
	for {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}

		var args []string
		if len(line) > 0 && line[0] == '*' {
			args, err = r.readArray(line)
		} else {
			args = strings.Fields(string(line))
		}
		if err != nil {
			return nil, err
		}
		if len(args) > 0 {
			return args, nil
		}
	}
}

func (r *Reader) readArray(header []byte) ([]string, error) {
	n, err := strconv.Atoi(string(header[1:]))
	if err != nil || n > maxArrayLen {
		return nil, fmt.Errorf("%w: invalid multibulk length", ErrProtocol)
	}
	if n <= 0 {
		return nil, nil
	}

	args := make([]string, 0, n)
	for range n {
		s, err := r.readBulk()
		if err != nil {
			return nil, err
		}
		args = append(args, s)
	}
	return args, nil
}

func (r *Reader) readBulk() (string, error) {
	line, err := r.readLine()
	if err != nil {
		return "", unexpected(err)
	}
	if len(line) == 0 || line[0] != '$' {
		got := "end of line"
		if len(line) > 0 {
			got = string(line[0])
		}
		return "", fmt.Errorf("%w: expected '$', got '%s'", ErrProtocol, got)
	}

	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n < 0 || n > maxBulkLen {
		return "", fmt.Errorf("%w: invalid bulk length", ErrProtocol)
	}

	var sb strings.Builder
	sb.Grow(min(n+2, 64*1024))
	if _, err := io.CopyN(&sb, r.rd, int64(n)+2); err != nil {
		return "", unexpected(err)
	}

	s := sb.String()
	if s[n:] != "\r\n" {
		return "", fmt.Errorf("%w: bulk string not terminated by CRLF", ErrProtocol)
	}
	return s[:n], nil
}

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.rd.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, fmt.Errorf("%w: too big inline request", ErrProtocol)
		}
		if len(line) > 0 {
			return nil, unexpected(err)
		}
		return nil, err
	}
	line = line[:len(line)-1]
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line, nil
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
