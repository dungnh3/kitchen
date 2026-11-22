package sse_test

import (
	"context"
	"io"
	"iter"
	"net/http"
	"testing"

	"github.com/dungnh3/kitchen/sse"
)

func BenchmarkAppendEvent(b *testing.B) {
	ev := sse.Event{ID: "1732089600000-12", Type: sse.TypeToken, Data: []byte(`{"event_id":"01J","data":"Hello, world"}`)}
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for b.Loop() {
		buf, _ = sse.AppendEvent(buf[:0], ev)
	}
	b.SetBytes(int64(len(buf)))
}

// countSource yields n messages then ends.
type countSource struct{ n int }

func (c countSource) Subscribe(_ context.Context, _, _ string) iter.Seq2[sse.Message, error] {
	m := sse.Message{ID: "1732089600000-12", Type: sse.TypeToken, Data: []byte("token ")}
	return func(yield func(sse.Message, error) bool) {
		for range c.n {
			if !yield(m, nil) {
				return
			}
		}
	}
}

func (countSource) CheckCursor(context.Context, string, string) error { return nil }

type discard struct{ h http.Header }

func (d *discard) Header() http.Header       { return d.h }
func (*discard) Write(p []byte) (int, error) { return io.Discard.Write(p) }
func (*discard) WriteHeader(int)             {}
func (*discard) Flush()                      {}

// BenchmarkServe measures the per-message cost of the HTTP write path
// (pump hand-off, visibility, encode, coalesced write+flush).
func BenchmarkServe(b *testing.B) {
	const perConn = 1000
	s, err := sse.NewServer(countSource{n: perConn}, sse.ServerConfig{})
	if err != nil {
		b.Fatal(err)
	}
	r, _ := http.NewRequestWithContext(b.Context(), http.MethodGet, "/", nil)
	b.ReportAllocs()
	for b.Loop() {
		w := &discard{h: http.Header{}}
		if err := s.Serve(w, r, "s", sse.ServeOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*perConn), "ns/msg")
}
