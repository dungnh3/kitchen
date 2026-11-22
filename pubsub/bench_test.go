package pubsub_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"
	tnoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/dungnh3/kitchen/pubsub"
)

// benchSource hands b.N messages to the engine as fast as it takes them and
// signals when all are settled.
type benchSource struct {
	msgs []*pubsub.Message
	wg   sync.WaitGroup
}

type benchDelivery struct {
	m  *pubsub.Message
	wg *sync.WaitGroup
}

func (d *benchDelivery) Message() *pubsub.Message { return d.m }
func (d *benchDelivery) Ack()                     { d.wg.Done() }
func (d *benchDelivery) Nack()                    { d.wg.Done() }

func (s *benchSource) System() string { return "bench" }

func (s *benchSource) Receive(ctx context.Context, fn func(context.Context, pubsub.Delivery)) error {
	for _, m := range s.msgs {
		fn(ctx, &benchDelivery{m: m, wg: &s.wg})
	}
	<-ctx.Done()
	return nil
}

func newBenchSource(n, keys int) *benchSource {
	s := &benchSource{msgs: make([]*pubsub.Message, n)}
	for i := range n {
		m := &pubsub.Message{ID: strconv.Itoa(i), Topic: "t", Data: []byte("x")}
		if keys > 0 {
			m.Key = "k" + strconv.Itoa(i%keys)
		}
		s.msgs[i] = m
	}
	s.wg.Add(n)
	return s
}

var benchTel = pubsub.WithTelemetry(pubsub.WithTracerProvider(tnoop.NewTracerProvider()), pubsub.WithMeterProvider(noop.NewMeterProvider()))

// BenchmarkSubscriber measures engine overhead per message (no-op handler,
// no-op telemetry).
func BenchmarkSubscriber(b *testing.B) {
	for _, tc := range []struct {
		name  string
		keys  int
		batch bool
	}{
		{name: "unkeyed"},
		{name: "keyed-100", keys: 100},
		{name: "batch-unkeyed", batch: true},
		{name: "batch-keyed-100", keys: 100, batch: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			src := newBenchSource(b.N, tc.keys)
			opts := []pubsub.Option{pubsub.WithLogger(quiet), benchTel, pubsub.WithConcurrency(32)}
			var s *pubsub.Subscriber
			if tc.batch {
				opts = append(opts, pubsub.WithBatch(100, 1e6))
				s, _ = pubsub.NewBatchSubscriber(src, func(context.Context, []*pubsub.Message) error { return nil }, opts...)
			} else {
				s, _ = pubsub.NewSubscriber(src, func(context.Context, *pubsub.Message) error { return nil }, opts...)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			b.ReportAllocs()
			b.ResetTimer()
			go func() { done <- s.Run(ctx) }()
			src.wg.Wait()
			b.StopTimer()
			cancel()
			if err := <-done; err != nil {
				b.Fatal(err)
			}
		})
	}
}
