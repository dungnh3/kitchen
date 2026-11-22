package pubsub_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
)

// recSource is a scripted Source: it hands over msgs eagerly (before any is
// settled), then each of later once every earlier message is settled, and
// records outcomes. adv, when set, is reported through RedeliveryAdvisor.
type recSource struct {
	msgs, later []*pubsub.Message
	adv         *pubsub.Redelivery
	gap         time.Duration // pause before each eager message after the first

	mu       sync.Mutex
	acks     []string
	nacks    []string
	settled  chan struct{}
	accepted atomic.Int64 // fn calls that returned
}

type advSource struct{ *recSource }

func (s advSource) Redelivery() pubsub.Redelivery { return *s.adv }

func (s *recSource) source() pubsub.Source {
	if s.adv != nil {
		return advSource{s}
	}
	return s
}

type recDelivery struct {
	s *recSource
	m *pubsub.Message
}

func (d recDelivery) Message() *pubsub.Message { return d.m }
func (d recDelivery) Ack()                     { d.s.record(&d.s.acks, d.m) }
func (d recDelivery) Nack()                    { d.s.record(&d.s.nacks, d.m) }

func (s *recSource) record(to *[]string, m *pubsub.Message) {
	s.mu.Lock()
	*to = append(*to, m.ID)
	s.mu.Unlock()
	s.settled <- struct{}{}
}

func (s *recSource) System() string { return "rec" }

func (s *recSource) Receive(ctx context.Context, fn func(context.Context, pubsub.Delivery)) error {
	n := 0
	for i, m := range s.msgs {
		if i > 0 && s.gap > 0 {
			time.Sleep(s.gap)
		}
		fn(ctx, recDelivery{s, m})
		s.accepted.Add(1)
	}
	for _, m := range s.later {
		for ; n < len(s.msgs); n++ {
			select {
			case <-s.settled:
			case <-ctx.Done():
				return nil
			}
		}
		fn(ctx, recDelivery{s, m})
		s.accepted.Add(1)
		n++
		s.msgs = append(s.msgs, m)
	}
	for ; n < len(s.msgs); n++ {
		select {
		case <-s.settled:
		case <-ctx.Done():
			return nil
		}
	}
	<-ctx.Done()
	return nil
}

func newRec(msgs ...*pubsub.Message) *recSource {
	return &recSource{msgs: msgs, settled: make(chan struct{}, 1024)}
}

func idMsg(id, key string) *pubsub.Message {
	return &pubsub.Message{ID: id, Key: key, Data: []byte(id), Topic: "t"}
}

// runUntilSettled runs s until every message of src was settled.
func runUntilSettled(t *testing.T, s *pubsub.Subscriber, src *recSource, total int) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	deadline := time.After(5 * time.Second)
	for {
		src.mu.Lock()
		n := len(src.acks) + len(src.nacks)
		src.mu.Unlock()
		if n >= total {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d of %d messages settled", n, total)
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// A message whose key had an earlier message fail during batch isolation
// must not run: running it applies its side effects before the earlier one.
func TestBatchIsolationKeepsKeyOrder(t *testing.T) {
	src := newRec(idMsg("a1", "k"), idMsg("a2", "k"))
	var mu sync.Mutex
	var calls [][]string
	h := func(_ context.Context, ms []*pubsub.Message) error {
		var ids []string
		for _, m := range ms {
			ids = append(ids, m.ID)
		}
		mu.Lock()
		calls = append(calls, ids)
		mu.Unlock()
		if slices.Contains(ids, "a1") {
			return errors.New("a1 is poison")
		}
		return nil
	}
	s, err := pubsub.NewBatchSubscriber(src.source(), h, fast(pubsub.WithConcurrency(1), pubsub.WithRetry(0, 0, 0), pubsub.WithBatch(2, time.Hour))...)
	if err != nil {
		t.Fatal(err)
	}
	runUntilSettled(t, s, src, 2)
	for _, c := range calls {
		if len(c) == 1 && c[0] == "a2" {
			t.Fatalf("a2 ran alone after a1 failed: calls %v", calls)
		}
	}
	if !slices.Contains(src.nacks, "a1") || !slices.Contains(src.nacks, "a2") {
		t.Fatalf("nacks %v, want a1 and a2", src.nacks)
	}
}

// A batch that fails as a whole because a dependency is down must not fan
// out into one handler call per message.
func TestBatchIsolationStopsOnSystemicFailure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		poison    string // only this message fails; "" = everything fails
		wantCalls int
		wantDead  []string
	}{
		{name: "outage", wantCalls: 1 + 2},
		{name: "one poison", poison: "3", wantCalls: 1 + 10, wantDead: []string{"3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var msgs []*pubsub.Message
			for i := range 10 {
				msgs = append(msgs, idMsg(fmt.Sprint(i), ""))
			}
			src := newRec(msgs...)
			var calls atomic.Int64
			var dead []string
			h := func(_ context.Context, ms []*pubsub.Message) error {
				calls.Add(1)
				for _, m := range ms {
					if tc.poison == "" {
						return errors.New("db unavailable")
					}
					if m.ID == tc.poison {
						if len(ms) == 1 {
							return pubsub.Permanent(errors.New("bad payload"))
						}
						return errors.New("batch failed")
					}
				}
				return nil
			}
			s, err := pubsub.NewBatchSubscriber(src.source(), h, fast(
				pubsub.WithConcurrency(1), pubsub.WithRetry(0, 0, 0), pubsub.WithBatch(10, time.Hour),
				pubsub.WithDeadLetter(func(_ context.Context, m *pubsub.Message, _ error) error {
					dead = append(dead, m.ID)
					return nil
				}))...)
			if err != nil {
				t.Fatal(err)
			}
			runUntilSettled(t, s, src, 10)
			if got := calls.Load(); got != int64(tc.wantCalls) {
				t.Fatalf("handler calls = %d, want %d", got, tc.wantCalls)
			}
			if !slices.Equal(dead, tc.wantDead) {
				t.Fatalf("dead-lettered %v, want %v", dead, tc.wantDead)
			}
		})
	}
}

// The key fence follows WithKeyFence and the source's Redelivery, and never
// fences on the broker's last delivery (the message goes to the broker's
// dead-letter topic and will not come back to lift the fence).
func TestKeyFenceModes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    pubsub.KeyFenceMode
		adv     *pubsub.Redelivery
		attempt int
		// wantB: B was queued behind A when A was finally nacked.
		// wantC: C arrived after A's nack (before its redelivery).
		wantB, wantC string
	}{
		{name: "auto without advisor", mode: pubsub.KeyFenceAuto, wantB: "nack", wantC: "nack"},
		{name: "auto advisor fence", mode: pubsub.KeyFenceAuto, adv: &pubsub.Redelivery{Fence: true}, wantB: "nack", wantC: "nack"},
		{name: "auto advisor no fence", mode: pubsub.KeyFenceAuto, adv: &pubsub.Redelivery{}, wantB: "nack", wantC: "ack"},
		{name: "on overrides advisor", mode: pubsub.KeyFenceOn, adv: &pubsub.Redelivery{}, wantB: "nack", wantC: "nack"},
		{name: "off", mode: pubsub.KeyFenceOff, wantB: "nack", wantC: "ack"},
		{name: "broker's last delivery", mode: pubsub.KeyFenceOn, adv: &pubsub.Redelivery{Fence: true, MaxDeliveryAttempts: 5}, attempt: 5, wantB: "ack", wantC: "ack"},
		{name: "before last delivery", mode: pubsub.KeyFenceAuto, adv: &pubsub.Redelivery{Fence: true, MaxDeliveryAttempts: 5}, attempt: 4, wantB: "nack", wantC: "nack"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := idMsg("A", "k")
			a.DeliveryAttempt = tc.attempt
			src := newRec(a, idMsg("B", "k"))
			src.later = []*pubsub.Message{idMsg("C", "k")}
			src.adv = tc.adv
			bQueued := make(chan struct{})
			go func() {
				for src.accepted.Load() < 2 {
					time.Sleep(time.Millisecond)
				}
				close(bQueued)
			}()
			h := func(_ context.Context, m *pubsub.Message) error {
				if m.ID == "A" {
					<-bQueued // B is queued behind A before A fails
					return errors.New("fails")
				}
				return nil
			}
			s, err := pubsub.NewSubscriber(src.source(), h, fast(pubsub.WithRetry(0, 0, 0), pubsub.WithKeyFence(tc.mode))...)
			if err != nil {
				t.Fatal(err)
			}
			runUntilSettled(t, s, src, 3)
			outcome := func(id string) string {
				if slices.Contains(src.acks, id) {
					return "ack"
				}
				return "nack"
			}
			if got := outcome("B"); got != tc.wantB {
				t.Errorf("B: %s, want %s", got, tc.wantB)
			}
			if got := outcome("C"); got != tc.wantC {
				t.Errorf("C: %s, want %s", got, tc.wantC)
			}
		})
	}
}

func TestKeyFenceValidation(t *testing.T) {
	h := func(context.Context, *pubsub.Message) error { return nil }
	for name, o := range map[string]pubsub.Option{
		"ttl 0":        pubsub.WithKeyFenceTTL(0),
		"invalid mode": pubsub.WithKeyFence(pubsub.KeyFenceMode(9)),
	} {
		if _, err := pubsub.NewSubscriber(newRec(), h, o); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

// The default in-flight window follows the concurrency, so one replica does
// not lease far more messages than it can work on.
func TestDefaultMaxInFlight(t *testing.T) {
	for _, tc := range []struct {
		name  string
		batch bool
		opts  []pubsub.Option
		want  int64
	}{
		{name: "single 2x concurrency", opts: []pubsub.Option{pubsub.WithConcurrency(3)}, want: 6},
		{name: "explicit", opts: []pubsub.Option{pubsub.WithConcurrency(3), pubsub.WithMaxInFlight(10)}, want: 10},
		{name: "batch concurrency x size", batch: true, opts: []pubsub.Option{pubsub.WithConcurrency(2), pubsub.WithBatch(4, time.Hour)}, want: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var msgs []*pubsub.Message
				for i := range 50 {
					msgs = append(msgs, idMsg(fmt.Sprint(i), ""))
				}
				src := newRec(msgs...)
				release := make(chan struct{})
				var s *pubsub.Subscriber
				var err error
				if tc.batch {
					s, err = pubsub.NewBatchSubscriber(src, func(context.Context, []*pubsub.Message) error { <-release; return nil }, fast(tc.opts...)...)
				} else {
					s, err = pubsub.NewSubscriber(src, func(context.Context, *pubsub.Message) error { <-release; return nil }, fast(tc.opts...)...)
				}
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, 1)
				go func() { done <- s.Run(ctx) }()
				synctest.Wait()
				if got := src.accepted.Load(); got != tc.want {
					t.Errorf("accepted %d messages before backpressure, want %d", got, tc.want)
				}
				close(release)
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			})
		})
	}
	h := func(context.Context, []*pubsub.Message) error { return nil }
	if _, err := pubsub.NewBatchSubscriber(newRec(), h, pubsub.WithBatch(100, time.Second), pubsub.WithMaxInFlight(10)); err == nil {
		t.Error("batch size above max in-flight: want error")
	}
}

func TestSubscriberClose(t *testing.T) {
	src := newRec(idMsg("1", ""))
	s, err := pubsub.NewSubscriber(src, func(context.Context, *pubsub.Message) error { return nil }, fast()...)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()
	for src.accepted.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	if err := s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run after Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Close")
	}
	if err := s.Close(t.Context()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := s.Run(context.Background()); !errors.Is(err, pubsub.ErrClosed) {
		t.Fatalf("second Run = %v, want ErrClosed", err)
	}

	s2, _ := pubsub.NewSubscriber(newRec(), func(context.Context, *pubsub.Message) error { return nil }, fast()...)
	if err := s2.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s2.Run(context.Background()); err != nil {
		t.Fatalf("Run after Close = %v, want nil (as for a cancelled ctx)", err)
	}
}

// An unkeyed message goes to an idle shard, not behind a busy one.
func TestBatchUnkeyedAvoidsBusyShard(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := newRec(idMsg("slow", ""), idMsg("quick", ""), idMsg("next", ""))
		src.gap = time.Second // quick is done before next arrives
		release := make(chan struct{})
		var mu sync.Mutex
		var handled []string
		h := func(_ context.Context, ms []*pubsub.Message) error {
			if ms[0].ID == "slow" {
				<-release
			}
			mu.Lock()
			handled = append(handled, ms[0].ID)
			mu.Unlock()
			return nil
		}
		s, err := pubsub.NewBatchSubscriber(src, h, fast(pubsub.WithConcurrency(2), pubsub.WithBatch(1, time.Hour))...)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		time.Sleep(3 * time.Second) // every message delivered
		synctest.Wait()
		mu.Lock()
		got := slices.Clone(handled)
		mu.Unlock()
		if !slices.Contains(got, "next") {
			t.Errorf("handled %v while slow blocks; next queued behind the busy shard", got)
		}
		close(release)
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
}

// Settlement outcomes, retries and the in-flight gauge are exported.
func TestSettlementMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	src := newRec(idMsg("ok", ""), idMsg("bad", ""))
	h := func(_ context.Context, m *pubsub.Message) error {
		if m.ID == "bad" {
			return errors.New("fails")
		}
		return nil
	}
	s, err := pubsub.NewSubscriber(src, h, fast(pubsub.WithRetry(1, 0, 0), pubsub.WithTelemetry(pubsub.WithMeterProvider(mp)))...)
	if err != nil {
		t.Fatal(err)
	}
	runUntilSettled(t, s, src, 2)
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	outcomes := map[string]int64{}
	var retries, inflight int64
	var buckets []float64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					switch m.Name {
					case "kitchen.pubsub.settled.messages":
						v, _ := p.Attributes.Value(attribute.Key("kitchen.pubsub.outcome"))
						outcomes[v.AsString()] += p.Value
					case "kitchen.pubsub.retries":
						retries += p.Value
					case "kitchen.pubsub.inflight.messages":
						inflight += p.Value
					}
				}
			case metricdata.Histogram[float64]:
				if m.Name == "messaging.process.duration" && len(d.DataPoints) > 0 {
					buckets = d.DataPoints[0].Bounds
				}
			}
		}
	}
	if outcomes["ack"] != 1 || outcomes["nack"] != 1 {
		t.Errorf("outcomes %v, want ack 1 nack 1", outcomes)
	}
	if retries != 1 {
		t.Errorf("retries %d, want 1", retries)
	}
	if inflight != 0 {
		t.Errorf("in-flight gauge %d after settling, want 0", inflight)
	}
	if len(buckets) == 0 || buckets[0] != 0.005 || buckets[len(buckets)-1] != 10 {
		t.Errorf("process duration buckets %v, want semconv 0.005..10s", buckets)
	}
}

// A burst of nacks writes a bounded number of warnings.
func TestNackLogSampled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var msgs []*pubsub.Message
		for i := range 200 {
			msgs = append(msgs, idMsg(fmt.Sprint(i), ""))
		}
		src := newRec(msgs...)
		var buf bytes.Buffer
		var mu sync.Mutex
		l := log.New(lockedWriter{&mu, &buf}, log.Options{})
		s, err := pubsub.NewSubscriber(src, func(context.Context, *pubsub.Message) error { return errors.New("down") },
			pubsub.WithLogger(l), pubsub.WithRetry(0, 0, 0), pubsub.WithMaxInFlight(500))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- s.Run(ctx) }()
		synctest.Wait()
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
		mu.Lock()
		n := strings.Count(buf.String(), "pubsub: message nacked")
		mu.Unlock()
		if n == 0 || n > 20 {
			t.Fatalf("%d nack warnings for 200 nacks, want 1..20", n)
		}
	})
}

type lockedWriter struct {
	mu *sync.Mutex
	b  *bytes.Buffer
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func TestDeadLetterTo(t *testing.T) {
	b := memory.New()
	dlq, err := b.Subscribe("orders.dlq", "dlq-sub")
	if err != nil {
		t.Fatal(err)
	}
	src := newRec(&pubsub.Message{ID: "m1", Key: "k", Topic: "orders-sub", Data: []byte("x"), DeliveryAttempt: 3, Attributes: map[string]string{"a": "b"}})
	s, err := pubsub.NewSubscriber(src, func(context.Context, *pubsub.Message) error {
		return pubsub.Permanent(errors.New("bad " + strings.Repeat("x", 1000)))
	}, fast(pubsub.WithDeadLetter(pubsub.DeadLetterTo(b, "orders.dlq")))...)
	if err != nil {
		t.Fatal(err)
	}
	runUntilSettled(t, s, src, 1)
	if !slices.Equal(src.acks, []string{"m1"}) {
		t.Fatalf("acks %v, want m1 acked after dead-lettering", src.acks)
	}
	if dlq.Pending() != 1 {
		t.Fatalf("dead-letter topic has %d messages, want 1", dlq.Pending())
	}
	var got *pubsub.Message
	ds, _ := pubsub.NewSubscriber(dlq, func(_ context.Context, m *pubsub.Message) error { got = m; return nil }, fast()...)
	stop := start(t, ds)
	eventually(t, func() bool { return dlq.Pending() == 0 }, "dlq not drained")
	_ = stop()
	want := map[string]string{
		"a": "b", pubsub.AttrDeadLetterSource: "orders-sub", pubsub.AttrDeadLetterID: "m1",
		pubsub.AttrDeadLetterAttempts: "3", pubsub.AttrDeadLetterKey: "k",
	}
	for k, v := range want {
		if got.Attributes[k] != v {
			t.Errorf("attribute %s = %q, want %q", k, got.Attributes[k], v)
		}
	}
	if c := got.Attributes[pubsub.AttrDeadLetterCause]; !strings.HasPrefix(c, "permanent: bad") || len(c) > 512 {
		t.Errorf("cause %q (len %d), want truncated permanent cause", c[:20], len(c))
	}
	if got.Key != "" {
		t.Errorf("dead-letter copy keyed %q; must be unkeyed", got.Key)
	}

	// A failed dead-letter publish nacks the message instead of losing it.
	_ = b.Close(t.Context())
	src2 := newRec(idMsg("m2", ""))
	s2, _ := pubsub.NewSubscriber(src2, func(context.Context, *pubsub.Message) error { return pubsub.Permanent(errors.New("bad")) },
		fast(pubsub.WithDeadLetter(pubsub.DeadLetterTo(b, "orders.dlq")))...)
	runUntilSettled(t, s2, src2, 1)
	if !slices.Equal(src2.nacks, []string{"m2"}) {
		t.Fatalf("nacks %v, want m2 nacked when the dead-letter publish fails", src2.nacks)
	}
}

// Property: with a broker that hands later messages of a key over while
// earlier ones are unacked (Pub/Sub, Kafka), random transient failures never
// let a message of a key succeed before an earlier message of that key.
func TestKeyOrderUnderPrefetchAndFailures(t *testing.T) {
	for seed := range 5 {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			b := memory.New(memory.WithPrefetch())
			sub, err := b.Subscribe("t", "s")
			if err != nil {
				t.Fatal(err)
			}
			const keys, perKey = 8, 25
			var msgs []*pubsub.Message
			for i := range perKey {
				for k := range keys {
					msgs = append(msgs, &pubsub.Message{Key: fmt.Sprint("k", k), Data: []byte(fmt.Sprint(i))})
				}
			}
			if _, err := b.Publish(t.Context(), "t", msgs...); err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			next := map[string]int{}
			var calls atomic.Int64
			var violation atomic.Value
			h := func(_ context.Context, m *pubsub.Message) error {
				n := calls.Add(1)
				if (n*2654435761+int64(seed))%5 == 0 { // ~20% transient failures
					return errors.New("transient")
				}
				mu.Lock()
				defer mu.Unlock()
				i, _ := strconv.Atoi(string(m.Data))
				if i != next[m.Key] {
					violation.CompareAndSwap(nil, fmt.Sprintf("key %s: handled %d, want %d", m.Key, i, next[m.Key]))
				}
				next[m.Key] = i + 1
				return nil
			}
			s, err := pubsub.NewSubscriber(sub, h, fast(pubsub.WithRetry(0, 0, 0), pubsub.WithConcurrency(4), pubsub.WithMaxInFlight(64))...)
			if err != nil {
				t.Fatal(err)
			}
			stop := start(t, s)
			eventually(t, func() bool { return sub.Pending() == 0 }, "pending %d", sub.Pending())
			_ = stop()
			if v := violation.Load(); v != nil {
				t.Fatal(v)
			}
		})
	}
}

// The same property in batch mode, with per-message failures, whole-batch
// failures and isolation. The handler, like a correct batch handler, fails
// the later messages of a key whose earlier message failed in the batch.
func TestBatchKeyOrderUnderPrefetchAndFailures(t *testing.T) {
	for seed := range 5 {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			b := memory.New(memory.WithPrefetch())
			sub, err := b.Subscribe("t", "s")
			if err != nil {
				t.Fatal(err)
			}
			const keys, perKey = 6, 30
			var msgs []*pubsub.Message
			for i := range perKey {
				for k := range keys {
					msgs = append(msgs, &pubsub.Message{Key: fmt.Sprint("k", k), Data: []byte(fmt.Sprint(i))})
				}
			}
			if _, err := b.Publish(t.Context(), "t", msgs...); err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			next := map[string]int{}
			var calls atomic.Int64
			var violation atomic.Value
			h := func(_ context.Context, ms []*pubsub.Message) error {
				n := calls.Add(1)
				if (n*40503+int64(seed))%7 == 0 {
					return errors.New("whole batch fails")
				}
				mu.Lock()
				defer mu.Unlock()
				failed := map[int]error{}
				bad := map[string]bool{}
				for i, m := range ms {
					if bad[m.Key] || (n*int64(i+3)+int64(seed))%6 == 0 {
						bad[m.Key] = true
						failed[i] = errors.New("transient")
						continue
					}
					v, _ := strconv.Atoi(string(m.Data))
					if v != next[m.Key] {
						violation.CompareAndSwap(nil, fmt.Sprintf("key %s: handled %d, want %d", m.Key, v, next[m.Key]))
					}
					next[m.Key] = v + 1
				}
				if len(failed) > 0 {
					return &pubsub.BatchError{Failed: failed}
				}
				return nil
			}
			s, err := pubsub.NewBatchSubscriber(sub, h, fast(pubsub.WithRetry(1, 0, 0), pubsub.WithConcurrency(3),
				pubsub.WithBatch(8, time.Millisecond))...)
			if err != nil {
				t.Fatal(err)
			}
			stop := start(t, s)
			eventually(t, func() bool { return sub.Pending() == 0 }, "pending %d", sub.Pending())
			_ = stop()
			if v := violation.Load(); v != nil {
				t.Fatal(v)
			}
		})
	}
}

// A source's consumer identity is on every consumer metric.
func TestConsumerIdentityOnMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	e := newEnv(t)
	s, err := pubsub.NewSubscriber(e.sub, func(context.Context, *pubsub.Message) error { return nil },
		fast(pubsub.WithTelemetry(pubsub.WithMeterProvider(mp)))...)
	if err != nil {
		t.Fatal(err)
	}
	stop := start(t, s)
	e.publish(msg("", "x"))
	eventually(t, func() bool { return e.sub.Pending() == 0 }, "not settled")
	_ = stop()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			var sets []attribute.Set
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					sets = append(sets, p.Attributes)
				}
			case metricdata.Histogram[float64]:
				for _, p := range d.DataPoints {
					sets = append(sets, p.Attributes)
				}
			}
			for _, as := range sets {
				if m.Name == "messaging.client.sent.messages" || m.Name == "messaging.client.operation.duration" {
					continue // producer side
				}
				v, ok := as.Value("messaging.destination.subscription.name")
				if !ok || v.AsString() != "orders-sub" {
					t.Errorf("%s: subscription attribute = %q, want orders-sub", m.Name, v.AsString())
				}
				checked++
			}
		}
	}
	if checked == 0 {
		t.Fatal("no consumer metric collected")
	}
}
