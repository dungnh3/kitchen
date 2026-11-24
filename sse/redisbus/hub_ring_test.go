package redisbus

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/sse"
)

// A browser reconnecting after the stream expired (DoneTTL/ActiveTTL) passes
// CheckCursor, so headers are committed. Subscribe must then report a replay
// gap (terminal stream.replay_gap event), not ErrInvalidCursor, which ends
// the response without a terminal event and makes EventSource reconnect
// forever.
func TestHubResumeExpiredStreamIsGap(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{})
	h := newHub(t, rdb, HubConfig{})
	ids := appendN(t, p, "r1", 0, 3)
	if _, err := p.End(t.Context(), "r1"); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Del(t.Context(), p.Keyspace().Key("r1")).Err(); err != nil { // expiry
		t.Fatal(err)
	}
	if err := h.CheckCursor(t.Context(), "r1", ids[1]); err != nil {
		t.Fatalf("CheckCursor = %v, want nil", err)
	}
	r := wait(t, collect(t.Context(), h, "r1", ids[1]))
	if !errors.Is(r.err, sse.ErrReplayGap) {
		t.Fatalf("err = %v, want ErrReplayGap", r.err)
	}
}

// A reader is retired once, even when a subscriber of a stream that already
// ended (and left the reader) releases it after the reader was retired: the
// group that replaced it stays registered, so later streams share its
// reader instead of starting another one (and another connection).
func TestHubReaderRetiredOnce(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{})
	h := newHub(t, rdb, HubConfig{})
	ctx := t.Context()

	subscribe := func(id string) (context.CancelFunc, <-chan result) {
		c, cancel := context.WithCancel(ctx)
		return cancel, collect(c, h, id, "")
	}
	// a's subscriber blocks on its first message, so it stays attached after
	// a ends.
	appendN(t, p, "a", 0, 1)
	gate := make(chan struct{})
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		for _, err := range h.Subscribe(ctx, "a", "") {
			if err != nil {
				return
			}
			<-gate
		}
	}()
	cancelB, doneB := subscribe("b")
	waitStreams(t, h, 2)
	stA := h.lookup("a")
	h.gmu.Lock()
	rd := stA.rd
	h.gmu.Unlock()
	// a ends: the reader drops it, but a's subscriber is still attached.
	if _, err := p.End(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "reader dropped a", func() bool {
		rd.mu.Lock()
		defer rd.mu.Unlock()
		return rd.streams[stA.key] == nil
	})
	// b leaves: the reader is now empty and retired.
	cancelB()
	<-doneB
	// c starts a new reader in a new group.
	cancelC, doneC := subscribe("c")
	waitStreams(t, h, 2)
	// a's subscriber leaves (its stream ended long ago).
	close(gate)
	<-doneA
	// d must share c's reader.
	cancelD, doneD := subscribe("d")
	waitStreams(t, h, 2)
	if rc, rd := h.lookup("c").rd, h.lookup("d").rd; rc != rd {
		t.Fatalf("c and d use different readers (%p, %p): a retired reader was retired twice", rc, rd)
	}
	cancelC()
	cancelD()
	<-doneC
	<-doneD
}

// An idle stream must not pin RingSize entries of memory up front; the ring
// grows with traffic.
func TestHubRingGrowsLazily(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{})
	h := newHub(t, rdb, HubConfig{RingSize: 8})
	ctx, cancel := context.WithCancel(t.Context())
	done := collect(ctx, h, "r1", "")
	waitStreams(t, h, 1)
	st := h.lookup("r1")
	st.mu.Lock()
	idle := cap(st.ring)
	st.mu.Unlock()
	if idle > 0 {
		t.Fatalf("idle ring cap = %d, want 0", idle)
	}
	appendN(t, p, "r1", 0, 20)
	waitFor(t, "ring full", func() bool { st.mu.Lock(); defer st.mu.Unlock(); return st.n == 8 })
	st.mu.Lock()
	got := cap(st.ring)
	st.mu.Unlock()
	if got > 8 {
		t.Fatalf("ring cap = %d, want <= RingSize 8", got)
	}
	cancel()
	<-done
}

func (h *Hub) lookup(id string) *stream {
	sh := h.shard(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.m[id]
}

func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s", what)
		}
		time.Sleep(2 * time.Millisecond) //nolint:forbidigo // polls a real (miniredis) reader
	}
}

// RingBytes bounds the ring by entry size; a slow subscriber still gets
// every entry (from Redis) in order.
func TestHubRingBytesBound(t *testing.T) {
	_, rdb := newRedis(t)
	p := newProducer(t, rdb, ProducerConfig{})
	h := newHub(t, rdb, HubConfig{RingSize: 512, RingBytes: 1000, CatchupBatch: 3})
	ctx := t.Context()
	gate := make(chan struct{})
	out := make(chan result, 1)
	go func() {
		var r result
		first := true
		for m, err := range h.Subscribe(ctx, "r1", "") {
			if err != nil {
				r.err = err
				break
			}
			if first {
				<-gate // fall behind the ring
				first = false
			}
			r.msgs = append(r.msgs, m)
		}
		out <- r
	}()
	waitStreams(t, h, 1)
	big := make([]byte, 300)
	for i := range 20 {
		data := append([]byte(fmt.Sprintf("t%d|", i)), big...)
		if _, err := p.Append(ctx, "r1", sse.Message{Type: sse.TypeToken, Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	huge := make([]byte, 5000) // larger than RingBytes on its own
	if _, err := p.Append(ctx, "r1", sse.Message{Type: sse.TypeToken, Data: append([]byte("t20|"), huge...)}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.End(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	st := h.lookup("r1")
	waitFor(t, "reader saw the end", func() bool { st.mu.Lock(); defer st.mu.Unlock(); return st.ended })
	st.mu.Lock()
	n, bytes := st.n, st.bytes
	st.mu.Unlock()
	if bytes > 1000 || n > 3 {
		t.Fatalf("ring holds %d entries / %d bytes, want <= 1000 bytes", n, bytes)
	}
	close(gate)
	r := wait(t, out)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if len(r.msgs) != 21 {
		t.Fatalf("got %d messages, want 21", len(r.msgs))
	}
	for i, m := range r.msgs {
		if want := fmt.Sprintf("t%d|", i); !strings.HasPrefix(string(m.Data), want) {
			t.Fatalf("message %d = %.8q, want prefix %q", i, m.Data, want)
		}
	}
}

// A viewer without Last-Event-ID on a head-trimmed stream first gets
// stream.truncated, then the retained entries. Needs real Redis (miniredis
// does not report entries-added).
func TestHubTruncatedMarker(t *testing.T) {
	rdb := realRedis(t)
	ks := Keyspace{Prefix: fmt.Sprintf("ssetest:%d:", time.Now().UnixNano())}
	p := newProducer(t, rdb, ProducerConfig{Keyspace: ks})
	h := newHub(t, rdb, HubConfig{Keyspace: ks})
	appendN(t, p, "full", 0, 3)
	appendN(t, p, "cut", 0, 10)
	if _, err := p.End(t.Context(), "full"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.End(t.Context(), "cut"); err != nil {
		t.Fatal(err)
	}
	if err := rdb.XTrimMaxLen(t.Context(), p.Keyspace().Key("cut"), 4).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rdb.Del(t.Context(), p.Keyspace().Key("full"), p.Keyspace().Key("cut")).Err() })

	r := wait(t, collect(t.Context(), h, "full", ""))
	if r.err != nil {
		t.Fatal(r.err)
	}
	wantSeq(t, datas(r.msgs), 0, 3)

	r = wait(t, collect(t.Context(), h, "cut", ""))
	if r.err != nil {
		t.Fatal(r.err)
	}
	if len(r.msgs) == 0 || r.msgs[0].Type != sse.TypeTruncated {
		t.Fatalf("first message = %+v, want %s", r.msgs, sse.TypeTruncated)
	}
	wantSeq(t, datas(r.msgs[1:]), 7, 3)
}

func realRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("KITCHEN_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("KITCHEN_TEST_REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("ping %s: %v", addr, err)
	}
	return rdb
}
