package pubsub

import (
	"maps"
	"slices"
	"sync"
)

const DefaultBufferSize = 1024

type Message struct {
	Channel, Payload string
}

type Broker struct {
	mu      sync.RWMutex
	subs    map[string]map[*Subscriber]struct{}
	bufSize int
}

func NewBroker(bufSize int) *Broker {
	return &Broker{subs: make(map[string]map[*Subscriber]struct{}), bufSize: bufSize}
}

type Subscriber struct {
	broker   *Broker
	ch       chan Message
	kicked   chan struct{}
	kickOnce sync.Once
	channels map[string]struct{}
}

func (b *Broker) NewSubscriber() *Subscriber {
	return &Subscriber{
		broker:   b,
		ch:       make(chan Message, b.bufSize),
		kicked:   make(chan struct{}),
		channels: make(map[string]struct{}),
	}
}

func (b *Broker) Publish(channel, payload string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	msg := Message{Channel: channel, Payload: payload}
	n := 0
	for s := range b.subs[channel] {
		select {
		case s.ch <- msg:
			n++
		default:
			s.kick()
		}
	}
	return n
}

func (s *Subscriber) Messages() <-chan Message {
	return s.ch
}

func (s *Subscriber) Kicked() <-chan struct{} {
	return s.kicked
}

func (s *Subscriber) kick() {
	s.kickOnce.Do(func() { close(s.kicked) })
}

func (s *Subscriber) Subscribe(channel string) int {
	b := s.broker
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.subs[channel] == nil {
		b.subs[channel] = make(map[*Subscriber]struct{})
	}
	b.subs[channel][s] = struct{}{}
	s.channels[channel] = struct{}{}
	return len(s.channels)
}

func (s *Subscriber) Unsubscribe(channel string) int {
	b := s.broker
	b.mu.Lock()
	defer b.mu.Unlock()

	s.unsubscribeLocked(channel)
	return len(s.channels)
}

func (s *Subscriber) Close() {
	b := s.broker
	b.mu.Lock()
	defer b.mu.Unlock()

	for channel := range s.channels {
		s.unsubscribeLocked(channel)
	}
}

func (s *Subscriber) Channels() []string {
	s.broker.mu.RLock()
	defer s.broker.mu.RUnlock()

	return slices.Sorted(maps.Keys(s.channels))
}

func (s *Subscriber) Count() int {
	s.broker.mu.RLock()
	defer s.broker.mu.RUnlock()

	return len(s.channels)
}

func (s *Subscriber) unsubscribeLocked(channel string) {
	delete(s.channels, channel)
	subs := s.broker.subs[channel]
	delete(subs, s)
	if len(subs) == 0 {
		delete(s.broker.subs, channel)
	}
}
