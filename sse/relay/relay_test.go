package relay_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/envelope"
	"github.com/dungnh3/kitchen/sse/redisbus"
	"github.com/dungnh3/kitchen/sse/relay"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

const outbox = "outbox:test"

func setup(t *testing.T) (*redis.Client, *redisbus.Producer) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	p, err := redisbus.NewProducer(rdb, redisbus.ProducerConfig{
		Keyspace: redisbus.Keyspace{Buckets: 4},
		Outbox:   &redisbus.OutboxConfig{Stream: outbox},
	})
	if err != nil {
		t.Fatal(err)
	}
	return rdb, p
}

// fakePub records published messages and fails on demand.
type fakePub struct {
	mu      sync.Mutex
	got     []relay.Message
	fail    func(m relay.Message) error
	resumed []string
}

func (f *fakePub) Publish(_ context.Context, m relay.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		if err := f.fail(m); err != nil {
			return err
		}
	}
	f.got = append(f.got, m)
	return nil
}

func (f *fakePub) ResumePublish(key string) {
	f.mu.Lock()
	f.resumed = append(f.resumed, key)
	f.mu.Unlock()
}

func (f *fakePub) snapshot() []relay.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.got)
}

func cfg(consumer string) relay.Config {
	return relay.Config{
		Stream: outbox, Buckets: 4, Group: "g", Consumer: consumer,
		Block: 20 * time.Millisecond, RetryBackoff: 5 * time.Millisecond, MaxRetryBackoff: 20 * time.Millisecond,
		Logger: slog.New(slog.DiscardHandler),
	}
}

func start(t *testing.T, rdb redis.UniversalClient, c relay.Config, pub relay.Publisher) *relay.Relay {
	t.Helper()
	r, err := relay.New(rdb, c, pub)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = r.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return r
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func publish(t *testing.T, p *redisbus.Producer, stream string, n int) {
	t.Helper()
	for i := range n {
		if _, err := p.Append(context.Background(), stream, sse.Message{Type: "run.step", Data: fmt.Appendf(nil, "%s-%d", stream, i)}); err != nil {
			t.Fatal(err)
		}
	}
}

func pendingCount(t *testing.T, rdb *redis.Client, r *relay.Relay) int64 {
	t.Helper()
	var n int64
	for _, s := range r.Shards() {
		p, err := rdb.XPending(context.Background(), s, "g").Result()
		if err == nil {
			n += p.Count
		}
	}
	return n
}

func TestRelayPublishesAndAcks(t *testing.T) {
	rdb, p := setup(t)
	pub := &fakePub{}
	r := start(t, rdb, cfg("c1"), pub)
	publish(t, p, "runA", 5)
	publish(t, p, "runB", 5)
	eventually(t, "all published", func() bool { return len(pub.snapshot()) == 10 })
	eventually(t, "all acked", func() bool { return pendingCount(t, rdb, r) == 0 })
	byKey := map[string][]string{}
	for _, m := range pub.snapshot() {
		if m.Attributes["stream"] != m.OrderingKey || m.Attributes["type"] != "run.step" || m.Attributes[relay.AttrOutboxID] == "" {
			t.Fatalf("attributes = %v", m.Attributes)
		}
		byKey[m.OrderingKey] = append(byKey[m.OrderingKey], string(m.Data))
	}
	for _, k := range []string{"runA", "runB"} {
		for i, d := range byKey[k] {
			if d != fmt.Sprintf("%s-%d", k, i) {
				t.Fatalf("order for %s: %v", k, byKey[k])
			}
		}
	}
}

func TestRelayHoldsFailingKeyInOrder(t *testing.T) {
	rdb, p := setup(t)
	var mu sync.Mutex
	failures := 3
	pub := &fakePub{fail: func(m relay.Message) error {
		mu.Lock()
		defer mu.Unlock()
		if m.OrderingKey == "bad" && failures > 0 {
			failures--
			return errors.New("unavailable")
		}
		return nil
	}}
	r := start(t, rdb, cfg("c1"), pub)
	publish(t, p, "bad", 4)
	publish(t, p, "good", 4)
	eventually(t, "all published", func() bool { return len(pub.snapshot()) == 8 })
	eventually(t, "all acked", func() bool { return pendingCount(t, rdb, r) == 0 })
	var bad []string
	for _, m := range pub.snapshot() {
		if m.OrderingKey == "bad" {
			bad = append(bad, string(m.Data))
		}
	}
	if !slices.Equal(bad, []string{"bad-0", "bad-1", "bad-2", "bad-3"}) {
		t.Fatalf("bad key order = %v", bad)
	}
	pub.mu.Lock()
	resumed := slices.Clone(pub.resumed)
	pub.mu.Unlock()
	if len(resumed) != 3 || resumed[0] != "bad" {
		t.Fatalf("resumed = %v", resumed)
	}
}

func TestRelayPoisonDLQAndReplay(t *testing.T) {
	rdb, p := setup(t)
	poison := errors.New("invalid argument")
	var mu sync.Mutex
	reject := true
	pub := &fakePub{fail: func(m relay.Message) error {
		mu.Lock()
		defer mu.Unlock()
		if reject && string(m.Data) == "x-0" {
			return poison
		}
		return nil
	}}
	c := cfg("c1")
	c.PoisonAttempts = 2
	c.Classify = func(err error) relay.Class {
		if errors.Is(err, poison) {
			return relay.Poison
		}
		return relay.Transient
	}
	r := start(t, rdb, c, pub)
	publish(t, p, "x", 2)
	shard := r.Shards()[0]
	for _, s := range r.Shards() {
		if n, _ := rdb.XLen(context.Background(), s).Result(); n > 0 {
			shard = s
		}
	}
	eventually(t, "dead-lettered", func() bool {
		n, _ := rdb.XLen(context.Background(), relay.DLQKey(shard)).Result()
		return n == 1
	})
	eventually(t, "x-1 published", func() bool { return len(pub.snapshot()) == 1 })
	dl, _ := rdb.XRange(context.Background(), relay.DLQKey(shard), "-", "+").Result()
	if dl[0].Values["reason"] != "poison" || dl[0].Values["stream"] != "x" {
		t.Fatalf("dlq entry = %v", dl[0].Values)
	}
	mu.Lock()
	reject = false
	mu.Unlock()
	n, err := r.Replay(context.Background(), 0)
	if err != nil || n != 1 {
		t.Fatalf("Replay = %d, %v", n, err)
	}
	eventually(t, "replayed entry published", func() bool { return len(pub.snapshot()) == 2 })
}

func TestRelayMalformedEntry(t *testing.T) {
	rdb, _ := setup(t)
	pub := &fakePub{}
	r := start(t, rdb, cfg("c1"), pub)
	shard := r.Shards()[2]
	eventually(t, "group created", func() bool {
		g, err := rdb.XInfoGroups(context.Background(), shard).Result()
		return err == nil && len(g) == 1
	})
	rdb.XAdd(context.Background(), &redis.XAddArgs{Stream: shard, Values: []any{"type", "x"}})
	eventually(t, "malformed dead-lettered", func() bool {
		n, _ := rdb.XLen(context.Background(), relay.DLQKey(shard)).Result()
		return n == 1
	})
	if len(pub.snapshot()) != 0 {
		t.Fatal("malformed entry published")
	}
}

func TestRelayTakesOverDeadConsumer(t *testing.T) {
	rdb, p := setup(t)
	ctx := context.Background()
	r0, _ := relay.New(rdb, cfg("dead"), &fakePub{})
	for _, s := range r0.Shards() {
		if err := rdb.XGroupCreateMkStream(ctx, s, "g", "0").Err(); err != nil {
			t.Fatal(err)
		}
	}
	publish(t, p, "run", 3)
	// The "dead" consumer reads everything and never acks.
	for _, s := range r0.Shards() {
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "g", Consumer: "dead", Streams: []string{s, ">"}, Block: -1})
	}
	time.Sleep(20 * time.Millisecond)
	c := cfg("live")
	c.MinIdle = 10 * time.Millisecond
	c.ReclaimInterval = 10 * time.Millisecond
	pub := &fakePub{}
	start(t, rdb, c, pub)
	eventually(t, "taken over", func() bool { return len(pub.snapshot()) == 3 })
}

func TestEnvelopeRoute(t *testing.T) {
	e, _ := envelope.NewBuilder("run-9", "app", "u").WithTenantID("t1").WithEventID("E1").
		BuildJSON("step.completed", map[string]string{"workspace_id": "w", "todo_id": "td", "traceparent": "00-x"})
	m, _ := e.Message()
	msg, err := relay.EnvelopeRoute("orch")(relay.Entry{ID: "1-0", Type: m.Type, Data: m.Data, StreamID: "run-9"})
	if err != nil {
		t.Fatal(err)
	}
	if msg.OrderingKey != "t1:w:run-9" {
		t.Fatalf("key = %q", msg.OrderingKey)
	}
	for k, want := range map[string]string{
		relay.AttrRunID: "run-9", relay.AttrEventID: "E1", relay.AttrStepID: "td", relay.AttrSource: "orch",
		relay.AttrEventType: "step.completed", relay.AttrOutboxID: "1-0", relay.AttrTraceParent: "00-x",
	} {
		if msg.Attributes[k] != want {
			t.Errorf("attr %s = %q, want %q", k, msg.Attributes[k], want)
		}
	}
	if _, err := relay.EnvelopeRoute("o")(relay.Entry{Data: []byte("nope")}); !errors.Is(err, relay.ErrUnroutable) {
		t.Fatalf("err = %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	rdb, _ := setup(t)
	for _, c := range []relay.Config{{}, {Stream: "a{b}", Group: "g", Consumer: "c"}, {Stream: "s"}, {Stream: "s", Group: "g", Consumer: "c", Buckets: -1}} {
		if _, err := relay.New(rdb, c, &fakePub{}); err == nil {
			t.Errorf("config %+v accepted", c)
		}
	}
	if _, err := relay.New(rdb, cfg("c"), nil); err == nil {
		t.Error("nil publisher accepted")
	}
}
