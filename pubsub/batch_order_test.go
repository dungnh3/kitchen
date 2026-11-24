package pubsub_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/pubsub"
)

// eagerSource hands every message to the engine at once, before any is
// settled, as Kafka does for records polled together. It honours the Source
// contract (nothing is delivered after a nack here) and records outcomes.
type eagerSource struct {
	msgs []*pubsub.Message
	mu   sync.Mutex
	acks []string
	nack []string
	done chan struct{}
	// delivered is closed once every message was handed to the engine.
	delivered chan struct{}
}

type eagerDelivery struct {
	s *eagerSource
	m *pubsub.Message
}

func (d eagerDelivery) Message() *pubsub.Message { return d.m }
func (d eagerDelivery) Ack()                     { d.s.settle(&d.s.acks, d.m) }
func (d eagerDelivery) Nack()                    { d.s.settle(&d.s.nack, d.m) }

func (s *eagerSource) settle(to *[]string, m *pubsub.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	*to = append(*to, string(m.Data))
	if len(s.acks)+len(s.nack) == len(s.msgs) {
		close(s.done)
	}
}

func (s *eagerSource) System() string { return "eager" }

func (s *eagerSource) Receive(ctx context.Context, fn func(context.Context, pubsub.Delivery)) error {
	for _, m := range s.msgs {
		fn(ctx, eagerDelivery{s: s, m: m})
	}
	close(s.delivered)
	<-ctx.Done()
	return nil
}

// Messages of a failed key that were delivered before the failure must be
// nacked even when they reach a later batch, with batches in between that
// do not contain the key.
func TestBatchFailedKeyAcrossLaterBatches(t *testing.T) {
	msg := func(key, data string) *pubsub.Message {
		return &pubsub.Message{ID: data, Key: key, Data: []byte(data), Topic: "t"}
	}
	src := &eagerSource{done: make(chan struct{}), delivered: make(chan struct{}), msgs: []*pubsub.Message{
		msg("k", "k1"), msg("k", "k2"), // batch 1: k1 fails, k2 nacked
		msg("", "x1"), msg("", "x2"), // batch 2: no k
		msg("k", "k3"), msg("", "x3"), // batch 3: k3 must not run before k1's redelivery
	}}
	var mu sync.Mutex
	var handled []string
	s, err := pubsub.NewBatchSubscriber(src, func(_ context.Context, ms []*pubsub.Message) error {
		// Every message is handed over before the first result: k3 is
		// buffered when k1 is nacked (the case this test pins down).
		<-src.delivered
		mu.Lock()
		defer mu.Unlock()
		failed := map[int]error{}
		for i, m := range ms {
			if string(m.Data) == "k1" {
				failed[i] = errors.New("transient")
				continue
			}
			handled = append(handled, string(m.Data))
		}
		if len(failed) > 0 {
			return &pubsub.BatchError{Failed: failed}
		}
		return nil
	}, append(fast(), pubsub.WithConcurrency(1), pubsub.WithBatch(2, time.Hour), pubsub.WithRetry(0, 0, 0), pubsub.WithMaxInFlight(100))...)
	if err != nil {
		t.Fatal(err)
	}
	stop := start(t, s)
	select {
	case <-src.done:
	case <-time.After(5 * time.Second):
		t.Fatal("not every message was settled")
	}
	_ = stop()
	mu.Lock()
	defer mu.Unlock()
	for _, h := range handled {
		if h == "k3" {
			t.Fatalf("k3 was handled before k1 was redelivered (handled %v)", handled)
		}
	}
	src.mu.Lock()
	defer src.mu.Unlock()
	if !contains(src.nack, "k3") || !contains(src.nack, "k2") || !contains(src.nack, "k1") {
		t.Fatalf("nacked %v, want k1 k2 k3", src.nack)
	}
}

func contains(ss []string, v string) bool {
	for _, s := range ss {
		if s == v {
			return true
		}
	}
	return false
}

// lateSource delivers k1 and k2, waits until k1 is nacked, then hands over
// k3 (as a Source can when it decided to deliver k3 just before the nack),
// and finally redelivers k1, k2 and k3 in order, like Kafka or SQS FIFO.
type lateSource struct {
	mu       sync.Mutex
	order    []string // handled order reported by the test handler
	nackedK1 chan struct{}
	nackOnce sync.Once
	finished chan struct{}
	settled  int
}

type lateDelivery struct {
	s     *lateSource
	m     *pubsub.Message
	final bool
}

func (d lateDelivery) Message() *pubsub.Message { return d.m }
func (d lateDelivery) Ack()                     { d.s.done(d) }
func (d lateDelivery) Nack() {
	if d.m.ID == "k1" {
		d.s.nackOnce.Do(func() { close(d.s.nackedK1) })
	}
	d.s.done(d)
}

func (s *lateSource) done(d lateDelivery) {
	if !d.final {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.settled++; s.settled == 3 {
		close(s.finished)
	}
}

func (s *lateSource) System() string { return "late" }

func (s *lateSource) Receive(ctx context.Context, fn func(context.Context, pubsub.Delivery)) error {
	msg := func(id string) *pubsub.Message {
		return &pubsub.Message{ID: id, Key: "k", Data: []byte(id), Topic: "t"}
	}
	fn(ctx, lateDelivery{s: s, m: msg("k1")})
	fn(ctx, lateDelivery{s: s, m: msg("k2")})
	select {
	case <-s.nackedK1:
	case <-ctx.Done():
		return nil
	}
	fn(ctx, lateDelivery{s: s, m: msg("k3")}) // handed over after the nack
	// Redelivery in order; these are the final deliveries.
	for _, id := range []string{"k1", "k2", "k3"} {
		fn(ctx, lateDelivery{s: s, m: msg(id), final: true})
	}
	<-ctx.Done()
	return nil
}

func TestKeyFenceLateHandover(t *testing.T) {
	for _, batch := range []bool{false, true} {
		name := map[bool]string{false: "single", true: "batch"}[batch]
		t.Run(name, func(t *testing.T) {
			src := &lateSource{nackedK1: make(chan struct{}), finished: make(chan struct{})}
			var mu sync.Mutex
			failedOnce := false
			handle := func(m *pubsub.Message) error {
				mu.Lock()
				defer mu.Unlock()
				if m.ID == "k1" && !failedOnce {
					failedOnce = true
					return errors.New("transient")
				}
				src.order = append(src.order, m.ID)
				return nil
			}
			opts := append(fast(), pubsub.WithRetry(0, 0, 0), pubsub.WithConcurrency(1))
			var s *pubsub.Subscriber
			var err error
			if batch {
				s, err = pubsub.NewBatchSubscriber(src, func(_ context.Context, ms []*pubsub.Message) error {
					failed := map[int]error{}
					for i, m := range ms {
						if e := handle(m); e != nil {
							failed[i] = e
						}
					}
					if len(failed) > 0 {
						return &pubsub.BatchError{Failed: failed}
					}
					return nil
				}, append(opts, pubsub.WithBatch(1, time.Millisecond))...)
			} else {
				s, err = pubsub.NewSubscriber(src, func(_ context.Context, m *pubsub.Message) error { return handle(m) }, opts...)
			}
			if err != nil {
				t.Fatal(err)
			}
			stop := start(t, s)
			select {
			case <-src.finished:
			case <-time.After(5 * time.Second):
				t.Fatal("redelivery not settled")
			}
			_ = stop()
			mu.Lock()
			defer mu.Unlock()
			// The test checks only k3: it must not run before the
			// redelivered k1.
			i1, i3 := index(src.order, "k1"), index(src.order, "k3")
			if i3 >= 0 && (i1 < 0 || i3 < i1) {
				t.Fatalf("k3 handled before k1: %v", src.order)
			}
		})
	}
}

func index(ss []string, v string) int {
	for i, s := range ss {
		if s == v {
			return i
		}
	}
	return -1
}
