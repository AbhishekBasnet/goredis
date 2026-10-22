package resp

import (
	"bytes"
	"io"
	"testing"
)

func BenchmarkReadCommand(b *testing.B) {
	cmd := []byte("*3\r\n$3\r\nSET\r\n$16\r\nkey:000000012345\r\n$3\r\nxxx\r\n")
	data := bytes.Repeat(cmd, 1000)
	b.SetBytes(int64(len(cmd)))
	b.ReportAllocs()

	r := bytes.NewReader(data)
	rd := NewReader(r)
	for i := 0; b.Loop(); i++ {
		if i%1000 == 0 {
			r.Reset(data)
		}
		if _, err := rd.ReadCommand(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteBulk(b *testing.B) {
	w := NewWriter(io.Discard)
	b.ReportAllocs()
	for b.Loop() {
		w.WriteBulk("value")
	}
	w.Flush()
}

func BenchmarkWriteInteger(b *testing.B) {
	w := NewWriter(io.Discard)
	b.ReportAllocs()
	for b.Loop() {
		w.WriteInteger(1234567)
	}
	w.Flush()
}
