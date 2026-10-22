package store

import (
	"strconv"
	"testing"
)

func BenchmarkSet(b *testing.B) {
	s := New()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		s.Set("key:"+strconv.Itoa(i%10000), "xxx", 0)
	}
}

func BenchmarkGet(b *testing.B) {
	s := New()
	s.Set("key", "xxx", 0)
	b.ReportAllocs()
	for b.Loop() {
		s.Get("key")
	}
}

func BenchmarkIncrBy(b *testing.B) {
	s := New()
	b.ReportAllocs()
	for b.Loop() {
		s.IncrBy("counter", 1)
	}
}

func BenchmarkGetParallel(b *testing.B) {
	s := New()
	s.Set("key", "xxx", 0)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get("key")
		}
	})
}

func BenchmarkSetParallel(b *testing.B) {
	s := New()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Set("key", "xxx", 0)
		}
	})
}

func BenchmarkPushPop(b *testing.B) {
	s := New()
	b.ReportAllocs()
	for b.Loop() {
		s.Push("list", false, "xxx")
		s.Pop("list", true, 1)
	}
}
