package redisbus

import (
	"fmt"
	"testing"

	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/internal/streamid"
)

// BenchmarkStreamPush measures the reader's per-entry cost of appending to a
// stream ring and waking subscribers.
func BenchmarkStreamPush(b *testing.B) {
	st := &stream{ring: make([]entry, DefaultRingSize), notify: make(chan struct{})}
	es := make([]entry, 16)
	seq := uint64(0)
	b.ReportAllocs()
	for b.Loop() {
		for i := range es {
			seq++
			es[i] = entry{id: streamid.ID{Ms: 1, Seq: seq}, msg: sse.Message{Type: sse.TypeToken}}
		}
		st.push(es)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(es)), "ns/entry")
}

// BenchmarkStreamAfter measures a subscriber's ring read.
func BenchmarkStreamAfter(b *testing.B) {
	st := &stream{ring: make([]entry, DefaultRingSize), notify: make(chan struct{})}
	es := make([]entry, DefaultRingSize)
	for i := range es {
		es[i] = entry{id: streamid.ID{Ms: 1, Seq: uint64(i) + 1}} //nolint:gosec // i >= 0
	}
	st.push(es)
	dst := make([]entry, 0, DefaultCatchupBatch)
	cur := streamid.ID{Ms: 1, Seq: DefaultRingSize - 16}
	b.ReportAllocs()
	for b.Loop() {
		dst = st.after(cur, dst[:0], DefaultCatchupBatch)
	}
	if len(dst) != 16 {
		b.Fatal(len(dst))
	}
}

func BenchmarkKey(b *testing.B) {
	ks, _ := Keyspace{}.withDefaults()
	ids := make([]string, 1024)
	for i := range ids {
		ids[i] = fmt.Sprintf("run_%d", i)
	}
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_ = ks.Key(ids[i&1023])
		i++
	}
}

// BenchmarkHubEndToEnd measures producer → hub → subscriber latency cost per
// message against miniredis (in-process; real Redis numbers come from the
// loadtest build tag).
func BenchmarkHubEndToEnd(b *testing.B) {
	_, rdb := newRedis(b)
	p := newProducer(b, rdb, ProducerConfig{})
	h := newHub(b, rdb, HubConfig{})
	ctx := b.Context()
	recv := make(chan struct{}, 1024)
	go func() {
		for _, err := range h.Subscribe(ctx, "bench", "") {
			if err != nil {
				return
			}
			recv <- struct{}{}
		}
	}()
	waitStreams(b, h, 1)
	msg := sse.Message{Type: sse.TypeToken, Data: []byte("token")}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := p.Append(ctx, "bench", msg); err != nil {
			b.Fatal(err)
		}
		<-recv
	}
}
