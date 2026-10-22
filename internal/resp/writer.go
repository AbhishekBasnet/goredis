package resp

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// Writer encodes RESP2 replies. Output is buffered: nothing reaches the
// client until Flush is called.
//
// The underlying bufio.Writer remembers the first error, so callers may
// ignore the errors from individual Write* calls and check Flush only.
type Writer struct {
	bw      *bufio.Writer
	scratch []byte // reused by writeInt to avoid an allocation per number
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w), scratch: make([]byte, 0, 20)}
}

// WriteSimple writes a simple string: +OK\r\n
func (w *Writer) WriteSimple(s string) error {
	return w.writeLine('+', s)
}

// WriteError writes an error: -ERR unknown command\r\n
// By convention msg starts with an error code such as ERR or WRONGTYPE.
func (w *Writer) WriteError(msg string) error {
	return w.writeLine('-', msg)
}

// WriteInteger writes an integer: :42\r\n
func (w *Writer) WriteInteger(n int64) error {
	return w.writeInt(':', n)
}

// WriteBulk writes a binary-safe bulk string: $5\r\nhello\r\n
func (w *Writer) WriteBulk(s string) error {
	w.writeInt('$', int64(len(s)))
	w.bw.WriteString(s)
	_, err := w.bw.WriteString("\r\n")
	return err
}

// WriteNull writes the null bulk string, which GET returns for a missing key: $-1\r\n
func (w *Writer) WriteNull() error {
	_, err := w.bw.WriteString("$-1\r\n")
	return err
}

// WriteArray writes an array header: *3\r\n
// The caller must then write exactly n elements with the other Write* methods,
// which allows arrays that mix types or contain nested arrays.
func (w *Writer) WriteArray(n int) error {
	return w.writeInt('*', int64(n))
}

// WriteNullArray writes the null array, which BLPOP returns on timeout: *-1\r\n
func (w *Writer) WriteNullArray() error {
	_, err := w.bw.WriteString("*-1\r\n")
	return err
}

// Flush sends everything buffered so far to the client.
func (w *Writer) Flush() error {
	return w.bw.Flush()
}

// writeLine writes a one-line reply. Simple strings and errors cannot contain
// CR or LF, so any are replaced with spaces rather than corrupting the stream.
func (w *Writer) writeLine(prefix byte, s string) error {
	if strings.ContainsAny(s, "\r\n") {
		s = strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
	}
	w.bw.WriteByte(prefix)
	w.bw.WriteString(s)
	_, err := w.bw.WriteString("\r\n")
	return err
}

func (w *Writer) writeInt(prefix byte, n int64) error {
	w.bw.WriteByte(prefix)
	w.scratch = strconv.AppendInt(w.scratch[:0], n, 10)
	w.bw.Write(w.scratch)
	_, err := w.bw.WriteString("\r\n")
	return err
}
