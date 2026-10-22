// Package resp implements the Redis serialization protocol (RESP2):
// a Reader that parses client commands and a Writer that encodes replies.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ErrProtocol is wrapped by every error caused by malformed client input.
// The server replies with the message and closes the connection, as Redis does.
//
// The message is capitalised on purpose: it matches Redis's own wording.
var ErrProtocol = errors.New("Protocol error")

// Limits from Redis. They stop a client from making us allocate huge buffers
// just by sending a big length prefix.
const (
	maxArrayLen = 1024 * 1024       // elements in one command
	maxBulkLen  = 512 * 1024 * 1024 // bytes in one argument
)

// Reader parses commands from a client connection.
type Reader struct {
	rd *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{rd: bufio.NewReader(r)}
}

// Buffered reports how many bytes are already read from the connection but
// not parsed yet. More than zero means the client pipelined more commands.
func (r *Reader) Buffered() int {
	return r.rd.Buffered()
}

// WaitReadable blocks until at least one more byte has arrived, without
// consuming it, and returns nil; or until reading fails, and returns the
// error. The server uses it to notice a client hanging up while a blocking
// command runs. It must not run at the same time as ReadCommand.
func (r *Reader) WaitReadable() error {
	_, err := r.rd.Peek(1)
	return err
}

// ReadCommand returns the next command as a list of arguments, e.g.
// ["SET", "key", "value"]. It never returns an empty command.
//
// Two formats are accepted, like real Redis:
//   - an array of bulk strings, which is what every client library sends:
//     *2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n
//   - an inline command, a plain line split on spaces, handy with nc/telnet:
//     ECHO hi\r\n
//
// It returns io.EOF when the client disconnects cleanly between commands,
// io.ErrUnexpectedEOF when it disconnects halfway through one, and an error
// wrapping ErrProtocol for malformed input.
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
		// Redis silently skips empty arrays and blank lines.
		if len(args) > 0 {
			return args, nil
		}
	}
}

// readArray parses the elements of an array whose header line ("*3") has
// already been read.
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

// readBulk parses one bulk string: "$5\r\nhello\r\n".
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

	// Copy instead of allocating n bytes up front: a client that claims
	// "$500000000" but sends nothing costs us almost no memory.
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

// readLine returns the next line without its "\r\n" (or bare "\n").
// The slice points into the bufio buffer, so it is only valid until the
// next read: callers must convert or parse it straight away.
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

// unexpected turns io.EOF into io.ErrUnexpectedEOF. Use it once part of a
// command has been read: hitting EOF then means the input was cut short.
func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}
