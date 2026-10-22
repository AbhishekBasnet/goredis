package resp

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

type Writer struct {
	bw      *bufio.Writer
	scratch []byte
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w), scratch: make([]byte, 0, 20)}
}

func (w *Writer) WriteSimple(s string) error {
	return w.writeLine('+', s)
}

func (w *Writer) WriteError(msg string) error {
	return w.writeLine('-', msg)
}

func (w *Writer) WriteInteger(n int64) error {
	return w.writeInt(':', n)
}

func (w *Writer) WriteBulk(s string) error {
	w.writeInt('$', int64(len(s)))
	w.bw.WriteString(s)
	_, err := w.bw.WriteString("\r\n")
	return err
}

func (w *Writer) WriteNull() error {
	_, err := w.bw.WriteString("$-1\r\n")
	return err
}

func (w *Writer) WriteArray(n int) error {
	return w.writeInt('*', int64(n))
}

func (w *Writer) WriteNullArray() error {
	_, err := w.bw.WriteString("*-1\r\n")
	return err
}

func (w *Writer) Flush() error {
	return w.bw.Flush()
}

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

func AppendArray(dst []byte, args ...string) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}
