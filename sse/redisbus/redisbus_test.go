package redisbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/sse"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func newRedis(t testing.TB) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func newProducer(t testing.TB, rdb redis.UniversalClient, cfg ProducerConfig) *Producer {
	t.Helper()
	p, err := NewProducer(rdb, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func newHub(t testing.TB, rdb redis.UniversalClient, cfg HubConfig) *Hub {
	t.Helper()
	if cfg.Block == 0 {
		cfg.Block = 50 * time.Millisecond
	}
	h, err := NewHub(rdb, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	return h
}

type result struct {
	msgs []sse.Message
	err  error
}

// collect subscribes and gathers messages until the sequence ends.
func collect(ctx context.Context, h *Hub, id, cursor string) <-chan result {
	out := make(chan result, 1)
	go func() {
		var r result
		for m, err := range h.Subscribe(ctx, id, cursor) {
			if err != nil {
				r.err = err
				break
			}
			r.msgs = append(r.msgs, m)
		}
		out <- r
	}()
	return out
}

func wait(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for subscriber")
		return result{}
	}
}

func appendN(t testing.TB, p *Producer, id string, from, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := from; i < from+n; i++ {
		eid, err := p.Append(t.Context(), id, sse.Message{Type: sse.TypeToken, Data: fmt.Appendf(nil, "t%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, eid)
	}
	return ids
}

func datas(ms []sse.Message) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		if m.Type == sse.TypeResumed {
			out = append(out, "<resumed>")
			continue
		}
		out = append(out, string(m.Data))
	}
	return out
}

func wantSeq(t *testing.T, got []string, from, n int) {
	t.Helper()
	if len(got) != n {
		t.Fatalf("got %d messages, want %d: %v", len(got), n, got)
	}
	for i, g := range got {
		if want := fmt.Sprintf("t%d", from+i); g != want {
			t.Fatalf("message %d = %q, want %q (all: %v)", i, g, want, got)
		}
	}
}

func TestKeyspace(t *testing.T) {
	ks, err := Keyspace{}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	k := ks.Key("run-1")
	b := ks.Bucket("run-1")
	if want := fmt.Sprintf("sse:{b%d}:run-1", b); k != want {
		t.Fatalf("Key = %q, want %q", k, want)
	}
	if got, want := ks.OutboxKey("outbox:o", "run-1"), fmt.Sprintf("outbox:o:{b%d}", b); got != want {
		t.Fatalf("OutboxKey = %q, want %q", got, want)
	}
	for _, bad := range []Keyspace{{Buckets: -1}, {Buckets: 20000}, {Prefix: "a{b}"}} {
		if _, err := bad.withDefaults(); err == nil {
			t.Errorf("%+v: want error", bad)
		}
	}
	for _, id := range []string{"", "a{b}", "x\ny"} {
		if validStreamID(id) == nil {
			t.Errorf("stream id %q accepted", id)
		}
	}
}

func TestProducerAppendEnd(t *testing.T) {
	mr, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{ActiveTTL: time.Hour, DoneTTL: time.Minute})
	ctx := context.Background()
	key := p.Keyspace().Key("r1")

	id1, err := p.Append(ctx, "r1", sse.Message{Type: "stream.token", Data: []byte("a")})
	if err != nil || id1 == "" {
		t.Fatalf("Append = %q, %v", id1, err)
	}
	if ttl := mr.TTL(key); ttl != time.Hour {
		t.Fatalf("active TTL = %v", ttl)
	}
	e1, err := p.End(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	e2, err := p.End(ctx, "r1")
	if err != nil || e2 != e1 {
		t.Fatalf("End not idempotent: %q then %q, %v", e1, e2, err)
	}
	if ttl := mr.TTL(key); ttl != time.Minute {
		t.Fatalf("done TTL = %v", ttl)
	}
	if _, err := p.Append(ctx, "r1", sse.Message{Type: "x", Data: nil}); !errors.Is(err, ErrStreamEnded) {
		t.Fatalf("Append after End: %v, want ErrStreamEnded", err)
	}
	if n, _ := rdb.XLen(ctx, key).Result(); n != 2 {
		t.Fatalf("XLEN = %d, want 2", n)
	}
}

func TestProducerValidation(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{MaxPayloadBytes: 4})
	ctx := context.Background()
	cases := []struct {
		id string
		m  sse.Message
	}{
		{"", sse.Message{Type: "a"}},
		{"r", sse.Message{Type: ""}},
		{"r", sse.Message{Type: endType}},
		{"r", sse.Message{Type: "a\nb"}},
		{"r", sse.Message{Type: "a", Data: []byte("12345")}},
	}
	for _, c := range cases {
		if _, err := p.Append(ctx, c.id, c.m); err == nil {
			t.Errorf("Append(%q, %+v) succeeded", c.id, c.m)
		}
	}
	if _, err := NewProducer(rdb, ProducerConfig{OutboxClient: rdb}); err == nil {
		t.Error("OutboxClient without Outbox accepted")
	}
	if _, err := NewProducer(nil, ProducerConfig{}); err == nil {
		t.Error("nil client accepted")
	}
}

func TestProducerMaxLen(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{MaxLen: 5})
	appendN(t, p, "r", 0, 50)
	n, _ := rdb.XLen(context.Background(), p.Keyspace().Key("r")).Result()
	if n > 50 || n < 5 {
		t.Fatalf("XLEN = %d", n)
	}
}

func TestProducerMirror(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{Outbox: &OutboxConfig{Stream: "outbox:t", MaxLen: 100}})
	ctx := context.Background()
	if _, err := p.Append(ctx, "r1", sse.Message{Type: "run.started", Data: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	msgs, err := rdb.XRange(ctx, p.Keyspace().OutboxKey("outbox:t", "r1"), "-", "+").Result()
	if err != nil || len(msgs) != 1 {
		t.Fatalf("outbox = %v, %v", msgs, err)
	}
	if msgs[0].Values["stream"] != "r1" || msgs[0].Values["type"] != "run.started" {
		t.Fatalf("outbox entry = %v", msgs[0].Values)
	}
}

func TestProducerSplit(t *testing.T) {
	_, rdb := newRedis(t)
	mr2 := miniredis.RunT(t)
	rdb2 := redis.NewClient(&redis.Options{Addr: mr2.Addr(), MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	t.Cleanup(func() { _ = rdb2.Close() })
	p := newProducer(t, rdb, ProducerConfig{Outbox: &OutboxConfig{Stream: "outbox:s"}, OutboxClient: rdb2})
	ctx := context.Background()
	if _, err := p.Append(ctx, "r1", sse.Message{Type: "a", Data: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if n, _ := rdb2.XLen(ctx, p.Keyspace().OutboxKey("outbox:s", "r1")).Result(); n != 1 {
		t.Fatalf("split outbox len = %d", n)
	}
	mr2.Close()
	id, err := p.Append(ctx, "r1", sse.Message{Type: "a", Data: []byte("2")})
	if id == "" || !errors.Is(err, ErrOutboxDiverged) {
		t.Fatalf("Append with dead outbox = %q, %v", id, err)
	}
}

func TestHubLive(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{})
	h := newHub(t, rdb, HubConfig{})
	ctx := t.Context()

	sub := collect(ctx, h, "r1", "")
	waitStreams(t, h, 1)
	appendN(t, p, "r1", 0, 20)
	if _, err := p.End(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	r := wait(t, sub)
	if r.err != nil {
		t.Fatal(r.err)
	}
	wantSeq(t, datas(r.msgs), 0, 20)
	for i, m := range r.msgs {
		if m.ID == "" || m.Type != sse.TypeToken {
			t.Fatalf("message %d = %+v", i, m)
		}
	}
}

func TestHubLateJoinerFullReplay(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{})
	h := newHub(t, rdb, HubConfig{RingSize: 4, CatchupBatch: 3})
	ctx := t.Context()

	appendN(t, p, "r1", 0, 10)
	first := collect(ctx, h, "r1", "")
	waitStreams(t, h, 1)
	second := collect(ctx, h, "r1", "")
	appendN(t, p, "r1", 10, 10)
	if _, err := p.End(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	for _, ch := range []<-chan result{first, second} {
		r := wait(t, ch)
		if r.err != nil {
			t.Fatal(r.err)
		}
		wantSeq(t, datas(r.msgs), 0, 20)
	}
}

func TestHubResume(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{})
	h := newHub(t, rdb, HubConfig{})
	ctx := t.Context()

	ids := appendN(t, p, "r1", 0, 10)
	sub := collect(ctx, h, "r1", ids[4])
	waitStreams(t, h, 1)
	appendN(t, p, "r1", 10, 2)
	if _, err := p.End(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	r := wait(t, sub)
	if r.err != nil {
		t.Fatal(r.err)
	}
	got := datas(r.msgs)
	// t5..t9 from catch-up, <resumed> once live, then t10..t11 (the marker can
	// come before or after the live entries depending on timing).
	var plain []string
	resumed := 0
	for _, g := range got {
		if g == "<resumed>" {
			resumed++
			continue
		}
		plain = append(plain, g)
	}
	if resumed != 1 {
		t.Fatalf("resumed markers = %d in %v", resumed, got)
	}
	wantSeq(t, plain, 5, 7)
}

func TestHubResumeEndedStream(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{})
	h := newHub(t, rdb, HubConfig{})
	ids := appendN(t, p, "r1", 0, 5)
	if _, err := p.End(t.Context(), "r1"); err != nil {
		t.Fatal(err)
	}
	r := wait(t, collect(t.Context(), h, "r1", ids[1]))
	if r.err != nil {
		t.Fatal(r.err)
	}
	wantSeq(t, datas(r.msgs), 2, 3)
}

func TestHubReplayGap(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{})
	h := newHub(t, rdb, HubConfig{})
	ids := appendN(t, p, "r1", 0, 10)
	// Trim exactly so the cursor disappears.
	if err := rdb.XTrimMaxLen(t.Context(), p.Keyspace().Key("r1"), 3).Err(); err != nil {
		t.Fatal(err)
	}
	r := wait(t, collect(t.Context(), h, "r1", ids[2]))
	if !errors.Is(r.err, sse.ErrReplayGap) {
		t.Fatalf("err = %v, want ErrReplayGap", r.err)
	}
}

func TestHubInvalidCursor(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{})
	h := newHub(t, rdb, HubConfig{})
	appendN(t, p, "r1", 0, 2)
	for _, c := range []string{"abc", "1-2-3", "$", ">", "99999999999999-0"} {
		r := wait(t, collect(t.Context(), h, "r1", c))
		if !errors.Is(r.err, sse.ErrInvalidCursor) {
			t.Errorf("cursor %q: err = %v, want ErrInvalidCursor", c, r.err)
		}
	}
	if h.ValidCursor("abc") || !h.ValidCursor("0") || !h.ValidCursor("1-0") {
		t.Error("ValidCursor mismatch")
	}
}

// A subscriber that falls far behind the ring catches up from Redis and loses
// nothing.
func TestHubSlowSubscriberNoLoss(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{})
	h := newHub(t, rdb, HubConfig{RingSize: 8, CatchupBatch: 5})
	ctx := t.Context()

	gate := make(chan struct{})
	out := make(chan result, 1)
	go func() {
		var r result
		for m, err := range h.Subscribe(ctx, "r1", "") {
			if err != nil {
				r.err = err
				break
			}
			if len(r.msgs) == 0 {
				<-gate // stall after the first message
			}
			r.msgs = append(r.msgs, m)
		}
		out <- r
	}()
	waitStreams(t, h, 1)
	appendN(t, p, "r1", 0, 1)
	time.Sleep(20 * time.Millisecond) // let the subscriber take t0 and stall
	appendN(t, p, "r1", 1, 99)
	if _, err := p.End(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	close(gate)
	r := wait(t, out)
	if r.err != nil {
		t.Fatal(r.err)
	}
	wantSeq(t, datas(r.msgs), 0, 100)
}

func TestHubManyStreamsManyReaders(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{})
	h := newHub(t, rdb, HubConfig{MaxKeysPerRead: 3})
	ctx := t.Context()
	const streams = 10
	subs := make([]<-chan result, streams)
	for i := range streams {
		subs[i] = collect(ctx, h, fmt.Sprintf("s%d", i), "")
	}
	waitStreams(t, h, streams)
	h.gmu.Lock()
	nReaders := 0
	for _, g := range h.groups {
		nReaders += len(g.readers)
	}
	h.gmu.Unlock()
	if nReaders != 4 {
		t.Fatalf("readers = %d, want 4", nReaders)
	}
	var wg sync.WaitGroup
	for i := range streams {
		wg.Go(func() {
			appendN(t, p, fmt.Sprintf("s%d", i), 0, 5) //nolint:contextcheck // helper uses t.Context
			_, _ = p.End(ctx, fmt.Sprintf("s%d", i))
		})
	}
	wg.Wait()
	for _, ch := range subs {
		r := wait(t, ch)
		if r.err != nil {
			t.Fatal(r.err)
		}
		wantSeq(t, datas(r.msgs), 0, 5)
	}
	// All subscribers left: every reader retires.
	deadline := time.Now().Add(2 * time.Second)
	for {
		h.gmu.Lock()
		n := len(h.groups)
		h.gmu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("groups left: %d", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestHubClose(t *testing.T) {
	_, rdb := newRedis(t)
	h, err := NewHub(rdb, HubConfig{Block: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	sub := collect(t.Context(), h, "r1", "")
	waitStreams(t, h, 1)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { _ = h.Run(ctx); close(done) }()
	cancel()
	<-done
	r := wait(t, sub)
	if !errors.Is(r.err, ErrHubClosed) {
		t.Fatalf("err = %v, want ErrHubClosed", r.err)
	}
	r = wait(t, collect(t.Context(), h, "r2", ""))
	if !errors.Is(r.err, ErrHubClosed) {
		t.Fatalf("after close err = %v", r.err)
	}
}

func TestHubGroupingCluster(t *testing.T) {
	_, rdb := newRedis(t)
	h := newHub(t, rdb, HubConfig{Keyspace: Keyspace{Buckets: 8}})
	st := &stream{id: "abc"}
	if g := h.groupOf(st); g != 0 {
		t.Fatalf("standalone group = %d", g)
	}
	h.cluster = true
	if g, want := h.groupOf(st), h.cfg.Keyspace.Bucket("abc"); g != want {
		t.Fatalf("cluster group = %d, want bucket %d", g, want)
	}
}

// waitStreams waits until n streams are registered with readers or ended.
func waitStreams(t testing.TB, h *Hub, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := 0
		for i := range h.shards {
			sh := &h.shards[i]
			sh.mu.Lock()
			for _, st := range sh.m {
				select {
				case <-st.ready:
					got++
				default:
				}
			}
			sh.mu.Unlock()
		}
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("registered streams = %d, want %d", got, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
