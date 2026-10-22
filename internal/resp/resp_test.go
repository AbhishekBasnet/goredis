package resp

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
)

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"array", "*1\r\n$4\r\nPING\r\n", []string{"PING"}},
		{"array with args", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n", []string{"SET", "k", "hello"}},
		{"empty bulk", "*2\r\n$4\r\nECHO\r\n$0\r\n\r\n", []string{"ECHO", ""}},
		{"binary safe", "*2\r\n$4\r\nECHO\r\n$4\r\na\r\nb\r\n", []string{"ECHO", "a\r\nb"}},
		{"inline", "PING\r\n", []string{"PING"}},
		{"inline bare LF", "ECHO hi\n", []string{"ECHO", "hi"}},
		{"inline extra spaces", "  SET   k  v \r\n", []string{"SET", "k", "v"}},
		{"skips blank lines", "\r\n\r\nPING\r\n", []string{"PING"}},
		{"skips empty array", "*0\r\n*1\r\n$4\r\nPING\r\n", []string{"PING"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewReader(strings.NewReader(tt.input)).ReadCommand()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReadCommandPartialReads(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	r := NewReader(iotest.OneByteReader(strings.NewReader(input)))

	got, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := []string{"SET", "key", "value"}; !slices.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestReadCommandPipelined(t *testing.T) {
	input := "*1\r\n$4\r\nPING\r\n" + "*2\r\n$4\r\nECHO\r\n$1\r\na\r\n" + "PING\r\n"
	r := NewReader(strings.NewReader(input))
	want := [][]string{{"PING"}, {"ECHO", "a"}, {"PING"}}

	for i, w := range want {
		got, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("command %d: unexpected error: %v", i, err)
		}
		if !slices.Equal(got, w) {
			t.Errorf("command %d: got %q, want %q", i, got, w)
		}
		if last := i == len(want)-1; last != (r.Buffered() == 0) {
			t.Errorf("command %d: Buffered() = %d", i, r.Buffered())
		}
	}
	if _, err := r.ReadCommand(); err != io.EOF {
		t.Errorf("after last command: got %v, want io.EOF", err)
	}
}

func TestReadCommandErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  error
	}{
		{"clean EOF", "", io.EOF},
		{"cut in header", "*2", io.ErrUnexpectedEOF},
		{"cut between args", "*2\r\n$4\r\nECHO\r\n", io.ErrUnexpectedEOF},
		{"cut in bulk", "*1\r\n$4\r\nPI", io.ErrUnexpectedEOF},
		{"bad array length", "*abc\r\n", ErrProtocol},
		{"huge array length", "*99999999\r\n", ErrProtocol},
		{"missing dollar", "*1\r\n:4\r\n", ErrProtocol},
		{"bad bulk length", "*1\r\n$x\r\n", ErrProtocol},
		{"negative bulk length", "*1\r\n$-1\r\n", ErrProtocol},
		{"huge bulk length", "*1\r\n$999999999999\r\n", ErrProtocol},
		{"bulk too long", "*1\r\n$2\r\nPING\r\n", ErrProtocol},
		{"inline too long", strings.Repeat("a", 5000), ErrProtocol},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tt.input)).ReadCommand()
			if !errors.Is(err, tt.want) {
				t.Errorf("got %v, want %v", err, tt.want)
			}
		})
	}
}

func TestWriter(t *testing.T) {
	tests := []struct {
		name  string
		write func(w *Writer)
		want  string
	}{
		{"simple", func(w *Writer) { w.WriteSimple("OK") }, "+OK\r\n"},
		{"simple strips CRLF", func(w *Writer) { w.WriteSimple("a\r\nb") }, "+a  b\r\n"},
		{"error", func(w *Writer) { w.WriteError("ERR bad") }, "-ERR bad\r\n"},
		{"integer", func(w *Writer) { w.WriteInteger(42) }, ":42\r\n"},
		{"negative integer", func(w *Writer) { w.WriteInteger(-7) }, ":-7\r\n"},
		{"bulk", func(w *Writer) { w.WriteBulk("hello") }, "$5\r\nhello\r\n"},
		{"empty bulk", func(w *Writer) { w.WriteBulk("") }, "$0\r\n\r\n"},
		{"binary bulk", func(w *Writer) { w.WriteBulk("a\r\nb") }, "$4\r\na\r\nb\r\n"},
		{"null", func(w *Writer) { w.WriteNull() }, "$-1\r\n"},
		{"null array", func(w *Writer) { w.WriteNullArray() }, "*-1\r\n"},
		{"empty array", func(w *Writer) { w.WriteArray(0) }, "*0\r\n"},
		{"mixed array", func(w *Writer) {
			w.WriteArray(3)
			w.WriteBulk("message")
			w.WriteInteger(1)
			w.WriteNull()
		}, "*3\r\n$7\r\nmessage\r\n:1\r\n$-1\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			tt.write(w)
			if buf.Len() != 0 {
				t.Fatal("wrote to the client before Flush")
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func encode(args []string) []byte {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteArray(len(args))
	for _, a := range args {
		w.WriteBulk(a)
	}
	w.Flush()
	return buf.Bytes()
}

func FuzzReadCommand(f *testing.F) {
	seeds := []string{
		"*1\r\n$4\r\nPING\r\n",
		"*2\r\n$4\r\nECHO\r\n$0\r\n\r\n",
		"PING\r\n",
		"*-1\r\n",
		"*1\r\n$-1\r\n",
		"*1\r\n$3\r\nab\r\n",
		"$4\r\nPING\r\n",
		"*2\r\n$1\r\na\r\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for {
			args, err := r.ReadCommand()
			if err != nil {
				return
			}
			if len(args) == 0 {
				t.Fatal("ReadCommand returned an empty command")
			}

			again, err := NewReader(bytes.NewReader(encode(args))).ReadCommand()
			if err != nil {
				t.Fatalf("re-reading %q: %v", args, err)
			}
			if !slices.Equal(args, again) {
				t.Fatalf("round trip changed %q into %q", args, again)
			}
		}
	})
}
