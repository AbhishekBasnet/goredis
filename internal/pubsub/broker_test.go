package pubsub

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

func receive(t *testing.T, s *Subscriber) Message {
	t.Helper()
	select {
	case m := <-s.Messages():
		return m
	case <-time.After(time.Second):
		t.Fatal("no message")
		return Message{}
	}
}

func TestPublishFanOut(t *testing.T) {
	b := NewBroker(8)
	a, c := b.NewSubscriber(), b.NewSubscriber()
	a.Subscribe("news")
	c.Subscribe("news")
	c.Subscribe("sport")

	if n := b.Publish("news", "hello"); n != 2 {
		t.Errorf("Publish(news) reached %d, want 2", n)
	}
	if n := b.Publish("sport", "goal"); n != 1 {
		t.Errorf("Publish(sport) reached %d, want 1", n)
	}
	if n := b.Publish("weather", "rain"); n != 0 {
		t.Errorf("Publish(weather) reached %d, want 0", n)
	}

	if m := receive(t, a); m != (Message{"news", "hello"}) {
		t.Errorf("a got %+v", m)
	}
	if m := receive(t, c); m != (Message{"news", "hello"}) {
		t.Errorf("c got %+v first", m)
	}
	if m := receive(t, c); m != (Message{"sport", "goal"}) {
		t.Errorf("c got %+v second", m)
	}
	if len(a.Messages()) != 0 {
		t.Error("a got a message for a channel it did not subscribe to")
	}
}

func TestSubscribeCounts(t *testing.T) {
	b := NewBroker(8)
	s := b.NewSubscriber()

	if n := s.Subscribe("a"); n != 1 {
		t.Errorf("first Subscribe = %d", n)
	}
	if n := s.Subscribe("a"); n != 1 {
		t.Errorf("repeat Subscribe = %d, want 1", n)
	}
	if n := s.Subscribe("b"); n != 2 {
		t.Errorf("second channel = %d", n)
	}
	if got := s.Channels(); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("Channels = %q", got)
	}
	if n := s.Unsubscribe("a"); n != 1 {
		t.Errorf("Unsubscribe = %d, want 1", n)
	}
	if n := s.Unsubscribe("never"); n != 1 {
		t.Errorf("Unsubscribe of unknown channel = %d, want 1", n)
	}
	if n := b.Publish("a", "x"); n != 0 {
		t.Errorf("Publish after Unsubscribe reached %d", n)
	}
}

func TestCloseDropsEverything(t *testing.T) {
	b := NewBroker(8)
	s := b.NewSubscriber()
	s.Subscribe("a")
	s.Subscribe("b")
	s.Close()

	if s.Count() != 0 {
		t.Errorf("Count after Close = %d", s.Count())
	}
	if len(b.subs) != 0 {
		t.Errorf("broker still tracks %d channels", len(b.subs))
	}
}

func TestSlowSubscriberIsKicked(t *testing.T) {
	b := NewBroker(4)
	slow, fast := b.NewSubscriber(), b.NewSubscriber()
	slow.Subscribe("c")
	fast.Subscribe("c")

	done := make(chan struct{})
	go func() {
		for range 4 + 10 {
			b.Publish("c", "x")
			<-fast.Messages()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a subscriber that never reads blocked the publisher")
	}

	select {
	case <-slow.Kicked():
	default:
		t.Fatal("slow subscriber was not kicked")
	}
	select {
	case <-fast.Kicked():
		t.Fatal("fast subscriber was kicked")
	default:
	}
	if n := len(slow.Messages()); n != 4 {
		t.Errorf("slow subscriber holds %d messages, want its buffer of 4", n)
	}
}

func TestConcurrentPublishSubscribe(t *testing.T) {
	const publishers, perPublisher = 8, 200
	b := NewBroker(publishers * perPublisher)
	subs := make([]*Subscriber, 8)
	for i := range subs {
		subs[i] = b.NewSubscriber()
		subs[i].Subscribe("c")
	}

	var wg sync.WaitGroup
	for p := range publishers {
		wg.Go(func() {
			for i := range perPublisher {
				b.Publish("c", fmt.Sprintf("%d-%d", p, i))
			}
		})
	}
	wg.Go(func() {
		extra := b.NewSubscriber()
		for i := range 100 {
			extra.Subscribe(fmt.Sprint(i))
		}
		extra.Close()
	})
	wg.Wait()

	for i, s := range subs {
		last := make(map[string]int)
		for range publishers * perPublisher {
			m := receive(t, s)
			var p, n int
			fmt.Sscanf(m.Payload, "%d-%d", &p, &n)
			if prev, ok := last[fmt.Sprint(p)]; ok && n != prev+1 {
				t.Fatalf("subscriber %d: publisher %d's messages out of order: %d after %d", i, p, n, prev)
			}
			last[fmt.Sprint(p)] = n
		}
	}
}

func TestCounts(t *testing.T) {
	b := NewBroker(8)
	a, c := b.NewSubscriber(), b.NewSubscriber()
	a.Subscribe("x")
	a.Subscribe("y")
	c.Subscribe("y")
	if clients, channels := b.Counts(); clients != 2 || channels != 2 {
		t.Errorf("Counts = %d, %d; want 2, 2", clients, channels)
	}

	a.Unsubscribe("x")
	a.Unsubscribe("never")
	if clients, channels := b.Counts(); clients != 2 || channels != 1 {
		t.Errorf("after one unsubscribe: %d, %d; want 2, 1", clients, channels)
	}
	a.Close()
	c.Unsubscribe("y")
	c.Unsubscribe("y")
	if clients, channels := b.Counts(); clients != 0 || channels != 0 {
		t.Errorf("after everyone left: %d, %d; want 0, 0", clients, channels)
	}
}
