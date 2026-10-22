package store

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
)

func TestListMatchesSlice(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	var l list
	var want []string

	for i := range 10_000 {
		v := strconv.Itoa(i)
		switch op := rng.IntN(4); {
		case op == 0:
			l.pushFront(v)
			want = slices.Insert(want, 0, v)
		case op == 1:
			l.pushBack(v)
			want = append(want, v)
		case op == 2 && len(want) > 0:
			if got := l.popFront(); got != want[0] {
				t.Fatalf("step %d: popFront = %q, want %q", i, got, want[0])
			}
			want = want[1:]
		case op == 3 && len(want) > 0:
			if got := l.popBack(); got != want[len(want)-1] {
				t.Fatalf("step %d: popBack = %q, want %q", i, got, want[len(want)-1])
			}
			want = want[:len(want)-1]
		}

		if l.len() != len(want) {
			t.Fatalf("step %d: len = %d, want %d", i, l.len(), len(want))
		}
		if len(want) > 0 {
			j := rng.IntN(len(want))
			if got := l.at(j); got != want[j] {
				t.Fatalf("step %d: at(%d) = %q, want %q", i, j, got, want[j])
			}
		}
	}
}
