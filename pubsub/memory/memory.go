// Package memory is an in-process message broker implementing
// [pubsub.Publisher] and [pubsub.Source], for tests and local development.
//
// Semantics follow Google Cloud Pub/Sub with message ordering enabled: every
// subscription of a topic receives a copy of each message published after the
// subscription was created; a keyed message is not delivered while an earlier
// message of the same key is outstanding; a nacked message is redelivered
// before later messages of its key and its DeliveryAttempt is incremented.
package memory

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/dungnh3/kitchen/pubsub"
)

// Option configures a Broker.
type Option func(*Broker)

// WithTelemetry configures publisher tracing and metrics.
func WithTelemetry(opts ...pubsub.TelemetryOption) Option {
	return func(b *Broker) { b.telOpts = append(b.telOpts, opts...) }
}

// WithNackDelay delays redelivery of a nacked message by d (default 0).
func WithNackDelay(d time.Duration) Option {
	return func(b *Broker) { b.nackDelay = d }
}

// Broker is an in-memory broker. The zero value is not usable; call New.
type Broker struct {
	telOpts   []pubsub.TelemetryOption
	tel       *pubsub.Telemetry
	nackDelay time.Duration

	mu     sync.Mutex
	seq    uint64
	closed bool
	topics map[string][]*Subscription
	subs   map[string]*Subscription
}

var _ pubsub.Publisher = (*Broker)(nil)

// New returns an empty broker.
func New(opts ...Option) *Broker {
	b := &Broker{topics: map[string][]*Subscription{}, subs: map[string]*Subscription{}}
	for _, o := range opts {
		o(b)
	}
	b.tel = pubsub.NewTelemetry("memory", b.telOpts...)
	return b
}

// Subscribe creates the subscription name on topic. It receives the messages
// published after this call.
func (b *Broker) Subscribe(topic, name string) (*Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, pubsub.ErrClosed
	}
	if _, ok := b.subs[name]; ok {
		return nil, fmt.Errorf("memory: subscription %q already exists", name)
	}
	s := &Subscription{name: name, topic: topic, nackDelay: b.nackDelay, keys: map[string]bool{}, wake: make(chan struct{}, 1)}
	b.subs[name] = s
	b.topics[topic] = append(b.topics[topic], s)
	return s, nil
}

// Publish implements [pubsub.Publisher].
func (b *Broker) Publish(ctx context.Context, topic string, msgs ...*pubsub.Message) ([]string, error) {
	return b.tel.Publish(ctx, topic, msgs, func(ctx context.Context, msgs []*pubsub.Message) ([]string, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return nil, pubsub.ErrClosed
		}
		subs := b.topics[topic]
		ids := make([]string, len(msgs))
		now := time.Now()
		entries := make([]pubsub.Message, len(msgs))
		for i, m := range msgs {
			b.seq++
			ids[i] = strconv.FormatUint(b.seq, 10)
			entries[i] = pubsub.Message{
				ID: ids[i], Topic: topic, Key: m.Key,
				Data: append([]byte(nil), m.Data...), Attributes: maps.Clone(m.Attributes),
				PublishTime: now,
			}
		}
		b.mu.Unlock()
		for _, s := range subs {
			s.enqueue(entries)
		}
		return ids, nil
	})
}

// Close makes later Publish and Subscribe calls fail with pubsub.ErrClosed.
// Pending messages stay deliverable.
func (b *Broker) Close(context.Context) error {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	return nil
}

// Subscription is a [pubsub.Source] for one subscription.
type Subscription struct {
	name, topic string
	nackDelay   time.Duration

	mu        sync.Mutex
	queue     []*entry
	keys      map[string]bool // keys with an outstanding delivery
	wake      chan struct{}
	receiving bool
}

var _ pubsub.Source = (*Subscription)(nil)

type entry struct {
	msg      pubsub.Message
	attempts  int
	out       bool
	notBefore time.Time
}

func (s *Subscription) enqueue(msgs []pubsub.Message) {
	s.mu.Lock()
	for _, m := range msgs {
		m.Attributes = maps.Clone(m.Attributes)
		s.queue = append(s.queue, &entry{msg: m})
	}
	s.mu.Unlock()
	s.notify()
}

func (s *Subscription) notify() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// System implements [pubsub.Source].
func (s *Subscription) System() string { return "memory" }

// Pending returns the number of messages not yet acked.
func (s *Subscription) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

// Receive implements [pubsub.Source]. Only one Receive may run at a time.
func (s *Subscription) Receive(ctx context.Context, fn func(context.Context, pubsub.Delivery)) error {
	s.mu.Lock()
	if s.receiving {
		s.mu.Unlock()
		return errors.New("memory: Receive already running")
	}
	s.receiving = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.receiving = false
		s.mu.Unlock()
	}()
	for {
		if ctx.Err() != nil {
			return nil
		}
		e, retryIn := s.next()
		if e == nil {
			var timer <-chan time.Time
			var t *time.Timer
			if retryIn > 0 {
				t = time.NewTimer(retryIn)
				timer = t.C
			}
			select {
			case <-ctx.Done():
			case <-s.wake:
			case <-timer:
			}
			if t != nil {
				t.Stop()
			}
			continue
		}
		m := e.msg
		m.Attributes = maps.Clone(e.msg.Attributes)
		m.DeliveryAttempt = e.attempts
		fn(ctx, &delivery{s: s, e: e, msg: &m})
	}
}

// next marks and returns the first deliverable entry. When none is ready it
// returns nil and, if a delayed redelivery is pending, how long until it is.
func (s *Subscription) next() (*entry, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	blocked := map[string]bool{}
	now := time.Now()
	var wait time.Duration
	for _, e := range s.queue {
		k := e.msg.Key
		if e.out {
			continue
		}
		if k != "" && (s.keys[k] || blocked[k]) {
			blocked[k] = true
			continue
		}
		if d := e.notBefore.Sub(now); d > 0 {
			if wait == 0 || d < wait {
				wait = d
			}
			if k != "" {
				blocked[k] = true
			}
			continue
		}
		e.out = true
		e.attempts++
		if k != "" {
			s.keys[k] = true
		}
		return e, 0
	}
	return nil, wait
}

type delivery struct {
	s    *Subscription
	e    *entry
	msg  *pubsub.Message
	once sync.Once
}

func (d *delivery) Message() *pubsub.Message { return d.msg }

func (d *delivery) Ack() {
	d.once.Do(func() {
		s := d.s
		s.mu.Lock()
		for i, e := range s.queue {
			if e == d.e {
				s.queue = append(s.queue[:i], s.queue[i+1:]...)
				break
			}
		}
		delete(s.keys, d.e.msg.Key)
		s.mu.Unlock()
		s.notify()
	})
}

func (d *delivery) Nack() {
	d.once.Do(func() {
		s := d.s
		s.mu.Lock()
		d.e.out = false
		if s.nackDelay > 0 {
			d.e.notBefore = time.Now().Add(s.nackDelay)
		}
		delete(s.keys, d.e.msg.Key)
		s.mu.Unlock()
		s.notify()
	})
}
