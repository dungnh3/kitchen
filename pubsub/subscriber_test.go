package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func fast(extra ...pubsub.Option) []pubsub.Option {
	return append([]pubsub.Option{
		pubsub.WithLogger(quiet),
		pubsub.WithRetry(2, time.Millisecond, 5*time.Millisecond),
		pubsub.WithShutdownTimeout(time.Second),
	}, extra...)
}

type env struct {
	t   *testing.T
	b   *memory.Broker
	sub *memory.Subscription
}

func newEnv(t *testing.T, opts ...memory.Option) *env {
	t.Helper()
	b := memory.New(opts...)
	sub, err := b.Subscribe("orders", "orders-sub")
	if err != nil {
		t.Fatal(err)
	}
	return &env{t: t, b: b, sub: sub}
}

func (e *env) publish(msgs ...*pubsub.Message) {
	e.t.Helper()
	if _, err := e.b.Publish(context.Background(), "orders", msgs...); err != nil {
		e.t.Fatal(err)
	}
}

// start runs s and returns a stop function that cancels and waits for Run.
func start(t *testing.T, s *pubsub.Subscriber) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("Run did not return")
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func eventually(t *testing.T, cond func() bool, msg string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: "+msg, args...)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func msg(key, data string) *pubsub.Message { return &pubsub.Message{Key: key, Data: []byte(data)} }

func TestSubscriberAcksOnSuccess(t *testing.T) {
	e := newEnv(t)
	var n atomic.Int32
	s, err := pubsub.NewSubscriber(e.sub, func(ctx context.Context, m *pubsub.Message) error {
		n.Add(1)
		return nil
	}, fast()...)
	if err != nil {
		t.Fatal(err)
	}
	stop := start(t, s)
	for i := range 50 {
		e.publish(msg("", fmt.Sprint(i)))
	}
	eventually(t, func() bool { return n.Load() == 50 && e.sub.Pending() == 0 }, "handled %d", n.Load())
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := s.Run(context.Background()); !errors.Is(err, pubsub.ErrClosed) {
		t.Fatalf("second Run = %v, want ErrClosed", err)
	}
}

func TestSubscriberSettlement(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name         string
		handler      func(calls int) error
		deadLetter   pubsub.DeadLetterFunc
		wantCalls    int32
		wantPending  int
		wantDeadLets int32
	}{
		{"retry then success", func(c int) error {
			if c < 3 {
				return boom
			}
			return nil
		}, nil, 3, 0, 0},
		{"permanent skips retry and dead-letters", func(int) error { return pubsub.Permanent(boom) }, nil, 1, 0, 1},
		{"panic is retried", func(c int) error {
			if c == 1 {
				panic("kaboom")
			}
			return nil
		}, nil, 2, 0, 0},
		{"dead-letter failure nacks", func(int) error { return fmt.Errorf("bad: %w", pubsub.ErrPermanent) },
			func(context.Context, *pubsub.Message, error) error { return boom }, 1, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, memory.WithNackDelay(time.Hour))
			var calls, dl atomic.Int32
			dlf := func(ctx context.Context, m *pubsub.Message, cause error) error {
				dl.Add(1)
				if !pubsub.IsPermanent(cause) {
					t.Errorf("cause %v is not permanent", cause)
				}
				if tt.deadLetter != nil {
					return tt.deadLetter(ctx, m, cause)
				}
				return nil
			}
			s, err := pubsub.NewSubscriber(e.sub, func(ctx context.Context, m *pubsub.Message) error {
				return tt.handler(int(calls.Add(1)))
			}, fast(pubsub.WithDeadLetter(dlf))...)
			if err != nil {
				t.Fatal(err)
			}
			stop := start(t, s)
			e.publish(msg("", "x"))
			eventually(t, func() bool {
				return calls.Load() == tt.wantCalls && dl.Load() == tt.wantDeadLets && e.sub.Pending() == tt.wantPending
			}, "calls=%d deadletters=%d pending=%d", calls.Load(), dl.Load(), e.sub.Pending())
			time.Sleep(20 * time.Millisecond)
			if calls.Load() != tt.wantCalls {
				t.Fatalf("calls = %d, want %d", calls.Load(), tt.wantCalls)
			}
			if err := stop(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSubscriberNackRedeliversThenMaxDeliveryAttempts(t *testing.T) {
	e := newEnv(t)
	var attempts []int
	var mu sync.Mutex
	var dead atomic.Int32
	s, err := pubsub.NewSubscriber(e.sub, func(ctx context.Context, m *pubsub.Message) error {
		mu.Lock()
		attempts = append(attempts, m.DeliveryAttempt)
		mu.Unlock()
		return errors.New("always")
	}, fast(
		pubsub.WithRetry(0, 0, 0),
		pubsub.WithMaxDeliveryAttempts(3),
		pubsub.WithDeadLetter(func(context.Context, *pubsub.Message, error) error { dead.Add(1); return nil }),
	)...)
	if err != nil {
		t.Fatal(err)
	}
	start(t, s)
	e.publish(msg("", "x"))
	eventually(t, func() bool { return dead.Load() == 1 && e.sub.Pending() == 0 }, "not dead-lettered")
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(attempts, []int{1, 2, 3}) {
		t.Fatalf("delivery attempts = %v, want [1 2 3]", attempts)
	}
}

func TestSubscriberPerKeyOrdering(t *testing.T) {
	e := newEnv(t)
	const keys, perKey = 8, 25
	var mu sync.Mutex
	got := map[string][]int{}
	active := map[string]int{}
	var maxParallel, cur atomic.Int32
	s, err := pubsub.NewSubscriber(e.sub, func(ctx context.Context, m *pubsub.Message) error {
		mu.Lock()
		active[m.Key]++
		if active[m.Key] > 1 {
			t.Errorf("key %s handled concurrently", m.Key)
		}
		mu.Unlock()
		c := cur.Add(1)
		for {
			mx := maxParallel.Load()
			if c <= mx || maxParallel.CompareAndSwap(mx, c) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		cur.Add(-1)
		var n int
		fmt.Sscan(string(m.Data), &n)
		mu.Lock()
		active[m.Key]--
		got[m.Key] = append(got[m.Key], n)
		mu.Unlock()
		return nil
	}, fast(pubsub.WithConcurrency(4))...)
	if err != nil {
		t.Fatal(err)
	}
	start(t, s)
	for i := range perKey {
		for k := range keys {
			e.publish(msg(fmt.Sprint("k", k), fmt.Sprint(i)))
		}
	}
	eventually(t, func() bool { return e.sub.Pending() == 0 }, "pending %d", e.sub.Pending())
	mu.Lock()
	defer mu.Unlock()
	for k, seq := range got {
		if len(seq) != perKey || !slices.IsSorted(seq) {
			t.Fatalf("key %s order = %v", k, seq)
		}
	}
	if maxParallel.Load() < 2 {
		t.Fatalf("different keys never ran in parallel (max %d)", maxParallel.Load())
	}
}

func TestSubscriberKeyedFailureKeepsOrder(t *testing.T) {
	e := newEnv(t)
	var mu sync.Mutex
	var handled []string
	failOnce := true
	s, err := pubsub.NewSubscriber(e.sub, func(ctx context.Context, m *pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		if string(m.Data) == "1" && failOnce {
			failOnce = false
			return errors.New("transient")
		}
		handled = append(handled, string(m.Data))
		return nil
	}, fast(pubsub.WithRetry(0, 0, 0))...)
	if err != nil {
		t.Fatal(err)
	}
	start(t, s)
	e.publish(msg("k", "1"), msg("k", "2"), msg("k", "3"))
	eventually(t, func() bool { return e.sub.Pending() == 0 }, "pending")
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(handled, []string{"1", "2", "3"}) {
		t.Fatalf("handled = %v", handled)
	}
}

func TestSubscriberShutdownCancelsSlowHandler(t *testing.T) {
	e := newEnv(t)
	started := make(chan struct{})
	var once sync.Once
	s, err := pubsub.NewSubscriber(e.sub, func(ctx context.Context, m *pubsub.Message) error {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return ctx.Err()
	}, pubsub.WithLogger(quiet), pubsub.WithShutdownTimeout(20*time.Millisecond), pubsub.WithHandlerTimeout(0))
	if err != nil {
		t.Fatal(err)
	}
	stop := start(t, s)
	e.publish(msg("", "slow"), msg("k", "queued1"), msg("k", "queued2"))
	<-started
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	// The slow message was nacked after the shutdown timeout; nothing is lost.
	if p := e.sub.Pending(); p != 3 {
		t.Fatalf("pending after shutdown = %d, want 3", p)
	}
}

func TestSubscriberBackpressure(t *testing.T) {
	e := newEnv(t)
	release := make(chan struct{})
	var inHandler atomic.Int32
	s, err := pubsub.NewSubscriber(e.sub, func(ctx context.Context, m *pubsub.Message) error {
		inHandler.Add(1)
		<-release
		return nil
	}, fast(pubsub.WithMaxInFlight(3), pubsub.WithConcurrency(10))...)
	if err != nil {
		t.Fatal(err)
	}
	start(t, s)
	for range 10 {
		e.publish(msg("", "x"))
	}
	eventually(t, func() bool { return inHandler.Load() == 3 }, "in handler %d", inHandler.Load())
	time.Sleep(20 * time.Millisecond)
	if n := inHandler.Load(); n != 3 {
		t.Fatalf("in handler = %d, want 3 (max in flight)", n)
	}
	close(release)
	eventually(t, func() bool { return e.sub.Pending() == 0 }, "pending %d", e.sub.Pending())
}

func TestNewSubscriberValidation(t *testing.T) {
	e := newEnv(t)
	h := func(context.Context, *pubsub.Message) error { return nil }
	for name, o := range map[string]pubsub.Option{
		"concurrency": pubsub.WithConcurrency(0),
		"inflight":    pubsub.WithMaxInFlight(0),
		"retries":     pubsub.WithRetry(-1, 0, 0),
		"timeout":     pubsub.WithHandlerTimeout(-1),
	} {
		if _, err := pubsub.NewSubscriber(e.sub, h, o); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	if _, err := pubsub.NewSubscriber(nil, h); err == nil {
		t.Error("nil source: want error")
	}
	if _, err := pubsub.NewSubscriber(e.sub, nil); err == nil {
		t.Error("nil handler: want error")
	}
	if _, err := pubsub.NewBatchSubscriber(e.sub, func(context.Context, []*pubsub.Message) error { return nil }, pubsub.WithBatch(0, time.Second)); err == nil {
		t.Error("batch size 0: want error")
	}
}

func TestJSON(t *testing.T) {
	type order struct{ ID int }
	e := newEnv(t)
	got := make(chan int, 1)
	var dead atomic.Int32
	s, err := pubsub.NewSubscriber(e.sub, pubsub.JSON(func(ctx context.Context, o order, m *pubsub.Message) error {
		got <- o.ID
		return nil
	}), fast(pubsub.WithDeadLetter(func(context.Context, *pubsub.Message, error) error { dead.Add(1); return nil }))...)
	if err != nil {
		t.Fatal(err)
	}
	start(t, s)
	m, err := pubsub.NewJSON(order{ID: 7})
	if err != nil {
		t.Fatal(err)
	}
	e.publish(m, msg("", "{not json"))
	if id := <-got; id != 7 {
		t.Fatalf("id = %d", id)
	}
	eventually(t, func() bool { return dead.Load() == 1 && e.sub.Pending() == 0 }, "malformed message not dead-lettered")
}

func TestTracePropagation(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	tel := []pubsub.TelemetryOption{pubsub.WithTracerProvider(tp), pubsub.WithPropagator(propagation.TraceContext{})}
	e := newEnv(t, memory.WithTelemetry(tel...))
	got := make(chan trace.SpanContext, 1)
	s, err := pubsub.NewSubscriber(e.sub, func(ctx context.Context, m *pubsub.Message) error {
		got <- trace.SpanContextFromContext(ctx)
		return nil
	}, fast(pubsub.WithTelemetry(tel...))...)
	if err != nil {
		t.Fatal(err)
	}
	stop := start(t, s)

	ctx, parent := tp.Tracer("test").Start(context.Background(), "request")
	orig := msg("", "x")
	if _, err := e.b.Publish(ctx, "orders", orig); err != nil {
		t.Fatal(err)
	}
	parent.End()
	if orig.Attributes != nil {
		t.Fatal("Publish mutated the caller's message")
	}
	sc := <-got
	if sc.TraceID() != parent.SpanContext().TraceID() {
		t.Fatalf("consumer trace %s != producer trace %s", sc.TraceID(), parent.SpanContext().TraceID())
	}
	_ = stop()
	kinds := map[string]trace.SpanKind{}
	for _, sp := range sr.Ended() {
		kinds[sp.Name()] = sp.SpanKind()
	}
	if kinds["send orders"] != trace.SpanKindProducer || kinds["process orders"] != trace.SpanKindConsumer {
		t.Fatalf("spans = %v", kinds)
	}
}
