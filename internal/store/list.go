package store

// list is a double-ended queue backed by a ring buffer: O(1) push and pop
// at both ends, and O(1) access by index, which LRANGE needs.
//
// The elements are buf[head], buf[head+1], ... wrapping around the end of
// buf, n of them in total.
type list struct {
	buf  []string
	head int
	n    int
}

func (l *list) len() int { return l.n }

// at returns the i-th element from the front. 0 <= i < l.n.
func (l *list) at(i int) string {
	return l.buf[(l.head+i)%len(l.buf)]
}

func (l *list) pushFront(v string) {
	l.grow()
	l.head = (l.head - 1 + len(l.buf)) % len(l.buf)
	l.buf[l.head] = v
	l.n++
}

func (l *list) pushBack(v string) {
	l.grow()
	l.buf[(l.head+l.n)%len(l.buf)] = v
	l.n++
}

// popFront and popBack must only be called on a non-empty list.
func (l *list) popFront() string {
	v := l.buf[l.head]
	l.buf[l.head] = "" // drop the reference so the string can be collected
	l.head = (l.head + 1) % len(l.buf)
	l.n--
	return v
}

func (l *list) popBack() string {
	i := (l.head + l.n - 1) % len(l.buf)
	v := l.buf[i]
	l.buf[i] = ""
	l.n--
	return v
}

func (l *list) pop(front bool) string {
	if front {
		return l.popFront()
	}
	return l.popBack()
}

// grow doubles the buffer when it is full, unwrapping the elements so the
// list starts at index 0 again.
func (l *list) grow() {
	if l.n < len(l.buf) {
		return
	}
	buf := make([]string, max(8, 2*len(l.buf)))
	for i := range l.n {
		buf[i] = l.at(i)
	}
	l.buf, l.head = buf, 0
}
