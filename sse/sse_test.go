package sse_test

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/sse"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestAppendEvent(t *testing.T) {
	tests := []struct {
		name string
		ev   sse.Event
		want string
		err  bool
	}{
		{"data only", sse.Event{Data: []byte("hi")}, "data: hi\n\n", false},
		{
			"all fields",
			sse.Event{ID: "1-0", Type: "stream.token", Data: []byte("x"), Retry: 3 * time.Second},
			"id: 1-0\nevent: stream.token\nretry: 3000\ndata: x\n\n", false,
		},
		{"multiline LF CR CRLF", sse.Event{Data: []byte("a\nb\rc\r\nd")}, "data: a\ndata: b\ndata: c\ndata: d\n\n", false},
		{"trailing newline", sse.Event{Data: []byte("a\n")}, "data: a\ndata: \n\n", false},
		{"empty data dispatches", sse.Event{Type: "stream.done", Data: []byte{}}, "event: stream.done\ndata: \n\n", false},
		{"comment", sse.Event{Comment: "ping"}, ": ping\n\n", false},
		{"retry only", sse.Event{Retry: time.Second}, "retry: 1000\n\n", false},
		{"id with newline", sse.Event{ID: "a\nb"}, "", true},
		{"id with NUL", sse.Event{ID: "a\x00"}, "", true},
		{"type with CR", sse.Event{Type: "a\rb"}, "", true},
		{"comment with LF", sse.Event{Comment: "a\nb"}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sse.AppendEvent(nil, tt.ev)
			if tt.err {
				if !errors.Is(err, sse.ErrInvalidEvent) {
					t.Fatalf("err = %v, want ErrInvalidEvent", err)
				}
				return
			}
			if err != nil || string(got) != tt.want {
				t.Fatalf("got %q, %v; want %q", got, err, tt.want)
			}
			var buf bytes.Buffer
			if _, err := tt.ev.WriteTo(&buf); err != nil || buf.String() != tt.want {
				t.Fatalf("WriteTo = %q, %v", buf.String(), err)
			}
		})
	}
}

func TestVisibleTypes(t *testing.T) {
	v := sse.VisibleTypes("run.started", "step.")
	for typ, want := range map[string]bool{
		sse.TypeToken: true, "run.started": true, "run.failed": false, "step.x": true, "internal": false,
	} {
		if got := v(typ); got != want {
			t.Errorf("visible(%q) = %v, want %v", typ, got, want)
		}
	}
	if sse.DefaultVisible("run.started") {
		t.Error("unknown type visible by default")
	}
}

// fakeSource yields items from a per-stream channel.
type fakeSource struct {
	mu      sync.Mutex
	streams map[string]chan item
	cursors []string
}

type item struct {
	m   sse.Message
	err error
}

func newFake() *fakeSource { return &fakeSource{streams: map[string]chan item{}} }

func (f *fakeSource) ch(id string) chan item {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.streams[id]
	if !ok {
		c = make(chan item, 1024)
		f.streams[id] = c
	}
	return c
}

func (f *fakeSource) send(id string, ms ...sse.Message) {
	for _, m := range ms {
		f.ch(id) <- item{m: m}
	}
}

func (f *fakeSource) Subscribe(ctx context.Context, id, cursor string) iter.Seq2[sse.Message, error] {
	f.mu.Lock()
	f.cursors = append(f.cursors, cursor)
	f.mu.Unlock()
	c := f.ch(id)
	return func(yield func(sse.Message, error) bool) {
		for {
			select {
			case it, ok := <-c:
				if !ok {
					return
				}
				if !yield(it.m, it.err) || it.err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}
}

func (f *fakeSource) CheckCursor(_ context.Context, _, c string) error {
	if strings.ContainsAny(c, "x") {
		return sse.ErrInvalidCursor
	}
	return nil
}

// recorder is a concurrency-safe streaming ResponseWriter.
type recorder struct {
	mu      sync.Mutex
	h       http.Header
	buf     bytes.Buffer
	flushes int
	failAt  int // fail writes once buf exceeds this many bytes (0 = never)
}

func newRecorder() *recorder { return &recorder{h: http.Header{}} }

func (r *recorder) Header() http.Header { return r.h }
func (r *recorder) WriteHeader(int)     {}
func (r *recorder) Write(b []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failAt > 0 && r.buf.Len() > r.failAt {
		return 0, errors.New("broken pipe")
	}
	return r.buf.Write(b)
}
func (r *recorder) Flush() { r.mu.Lock(); r.flushes++; r.mu.Unlock() }
func (r *recorder) flushCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.flushes
}

func (r *recorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.String()
}

func newServer(t *testing.T, src sse.Source, cfg sse.ServerConfig) *sse.Server {
	t.Helper()
	s, err := sse.NewServer(src, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func request(ctx context.Context, lastID string) *http.Request {
	r := httptest.NewRequestWithContext(ctx, http.MethodGet, "/s", nil)
	if lastID != "" {
		r.Header.Set("Last-Event-ID", lastID)
	}
	return r
}

func TestServeStreamsAndEnds(t *testing.T) {
	f := newFake()
	s := newServer(t, f, sse.ServerConfig{})
	f.send("r1",
		sse.Message{ID: "1-0", Type: sse.TypeStarted, Data: []byte(`{}`)},
		sse.Message{ID: "2-0", Type: "internal.secret", Data: []byte(`x`)},
		sse.Message{ID: "3-0", Type: sse.TypeToken, Data: []byte("hello\nworld")},
	)
	close(f.ch("r1"))
	w := newRecorder()
	if err := s.Serve(w, request(t.Context(), "5-0"), "r1", sse.ServeOptions{}); err != nil {
		t.Fatal(err)
	}
	want := "retry: 3000\n\n" +
		"id: 1-0\nevent: stream.started\ndata: {}\n\n" +
		"id: 3-0\nevent: stream.token\ndata: hello\ndata: world\n\n" +
		"event: stream.done\ndata: \n\n"
	if got := w.String(); got != want {
		t.Fatalf("body:\n%q\nwant:\n%q", got, want)
	}
	if ct := w.h.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if w.h.Get("Connection") != "" {
		t.Fatal("Connection header must not be set (invalid on HTTP/2)")
	}
	if f.cursors[0] != "5-0" {
		t.Fatalf("cursor = %q", f.cursors[0])
	}
}

func TestServeNoDuplicateDone(t *testing.T) {
	f := newFake()
	s := newServer(t, f, sse.ServerConfig{})
	f.send("r", sse.Message{ID: "1-0", Type: sse.TypeDone, Data: []byte(`{"finish_reason":"stop"}`)})
	close(f.ch("r"))
	w := newRecorder()
	if err := s.Serve(w, request(t.Context(), ""), "r", sse.ServeOptions{}); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(w.String(), "event: stream.done"); n != 1 {
		t.Fatalf("stream.done count = %d:\n%s", n, w.String())
	}
}

func TestServeTransforms(t *testing.T) {
	f := newFake()
	s := newServer(t, f, sse.ServerConfig{})
	f.send("r",
		sse.Message{ID: "1-0", Type: sse.TypeToken, Data: []byte("ok")},
		sse.Message{ID: "2-0", Type: sse.TypeToken, Data: []byte("skip-me")},
		sse.Message{ID: "3-0", Type: sse.TypeToken, Data: []byte("bad")},
		sse.Message{ID: "4-0", Type: sse.TypeToken, Data: []byte("never")},
	)
	blocker := func(context.Context) sse.Transform {
		return func(_ context.Context, m *sse.Message) (sse.Action, error) {
			switch string(m.Data) {
			case "skip-me":
				return sse.ActionSkip, nil
			case "bad":
				*m = sse.Message{ID: m.ID, Type: "safety.blocked", Data: []byte(`{"reason":"policy"}`)}
				return sse.ActionStop, nil
			}
			return sse.ActionPass, nil
		}
	}
	w := newRecorder()
	err := s.Serve(w, request(t.Context(), ""), "r", sse.ServeOptions{Transforms: []sse.TransformFactory{blocker}})
	if err != nil {
		t.Fatal(err)
	}
	body := w.String()
	if !strings.Contains(body, "data: ok") || strings.Contains(body, "skip-me") || strings.Contains(body, "never") ||
		!strings.Contains(body, "event: safety.blocked") || strings.Contains(body, "stream.done") {
		t.Fatalf("body:\n%s", body)
	}
}

func TestServeTransformError(t *testing.T) {
	f := newFake()
	s := newServer(t, f, sse.ServerConfig{})
	f.send("r", sse.Message{ID: "1-0", Type: sse.TypeToken})
	boom := errors.New("boom")
	tf := func(context.Context) sse.Transform {
		return func(context.Context, *sse.Message) (sse.Action, error) { return 0, boom }
	}
	err := s.Serve(newRecorder(), request(t.Context(), ""), "r", sse.ServeOptions{Transforms: []sse.TransformFactory{tf}})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestServeInvalidCursorUncommitted(t *testing.T) {
	s := newServer(t, newFake(), sse.ServerConfig{})
	w := newRecorder()
	err := s.Serve(w, request(t.Context(), "x"), "r", sse.ServeOptions{})
	if !errors.Is(err, sse.ErrInvalidCursor) {
		t.Fatalf("err = %v", err)
	}
	if w.String() != "" || w.h.Get("Content-Type") != "" || w.flushes != 0 {
		t.Fatal("response was committed")
	}
}

func TestServeReplayGap(t *testing.T) {
	f := newFake()
	s := newServer(t, f, sse.ServerConfig{})
	f.ch("r") <- item{err: sse.ErrReplayGap}
	w := newRecorder()
	err := s.Serve(w, request(t.Context(), "1-0"), "r", sse.ServeOptions{})
	if !errors.Is(err, sse.ErrReplayGap) || !strings.Contains(w.String(), "event: stream.replay_gap") {
		t.Fatalf("err = %v, body:\n%s", err, w.String())
	}
}

func TestServeSourceError(t *testing.T) {
	f := newFake()
	s := newServer(t, f, sse.ServerConfig{})
	boom := errors.New("redis down")
	f.send("r", sse.Message{ID: "1-0", Type: sse.TypeToken, Data: []byte("a")})
	f.ch("r") <- item{err: boom}
	w := newRecorder()
	if err := s.Serve(w, request(t.Context(), ""), "r", sse.ServeOptions{}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(w.String(), "data: a") {
		t.Fatal("events before the error were not written")
	}
}

func TestServeClientDisconnect(t *testing.T) {
	f := newFake()
	s := newServer(t, f, sse.ServerConfig{})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Serve(newRecorder(), request(ctx, ""), "r", sse.ServeOptions{}) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("err = %v, want nil on disconnect", err)
	}
}

func TestServeWriteError(t *testing.T) {
	f := newFake()
	s := newServer(t, f, sse.ServerConfig{})
	for range 10 {
		f.send("r", sse.Message{ID: "1-0", Type: sse.TypeToken, Data: bytes.Repeat([]byte("x"), 100)})
	}
	w := newRecorder()
	w.failAt = 5 // the retry hint succeeds, the first data write fails
	err := s.Serve(w, request(t.Context(), ""), "r", sse.ServeOptions{})
	if err == nil || !strings.Contains(err.Error(), "broken pipe") {
		t.Fatalf("err = %v", err)
	}
}

func TestServeCoalescesWrites(t *testing.T) {
	f := newFake()
	s := newServer(t, f, sse.ServerConfig{})
	for range 100 {
		f.send("r", sse.Message{ID: "1-0", Type: sse.TypeToken, Data: []byte("t")})
	}
	close(f.ch("r"))
	w := newRecorder()
	if err := s.Serve(w, request(t.Context(), ""), "r", sse.ServeOptions{}); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(w.String(), "data: t\n"); n != 100 {
		t.Fatalf("events = %d", n)
	}
	if w.flushes > 20 {
		t.Fatalf("flushes = %d, want coalesced writes", w.flushes)
	}
}

func TestServeHeartbeatAndIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake()
		s := newServer(t, f, sse.ServerConfig{Heartbeat: 10 * time.Second})
		w := newRecorder()
		done := make(chan error, 1)
		go func() {
			done <- s.Serve(w, request(t.Context(), ""), "r", sse.ServeOptions{MaxIdle: 25 * time.Second})
		}()
		time.Sleep(11 * time.Second)
		synctest.Wait()
		if n := strings.Count(w.String(), ": ping\n"); n != 1 {
			t.Fatalf("pings after 11s = %d", n)
		}
		f.send("r", sse.Message{ID: "1-0", Type: sse.TypeToken, Data: []byte("a")})
		time.Sleep(9 * time.Second) // activity postpones the heartbeat
		synctest.Wait()
		if n := strings.Count(w.String(), ": ping\n"); n != 1 {
			t.Fatalf("pings after activity = %d", n)
		}
		time.Sleep(30 * time.Second)
		if err := <-done; !errors.Is(err, sse.ErrIdle) {
			t.Fatalf("err = %v, want ErrIdle", err)
		}
		if !strings.Contains(w.String(), `{"type":"idle_timeout"}`) {
			t.Fatalf("body:\n%s", w.String())
		}
	})
}

func TestServeFlushInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake()
		s := newServer(t, f, sse.ServerConfig{})
		w := newRecorder()
		done := make(chan error, 1)
		go func() {
			done <- s.Serve(w, request(t.Context(), ""), "r", sse.ServeOptions{FlushInterval: 50 * time.Millisecond})
		}()
		synctest.Wait()
		base := w.flushCount()
		f.send("r", sse.Message{ID: "1-0", Type: sse.TypeToken, Data: []byte("a")})
		synctest.Wait()
		if w.flushCount() != base {
			t.Fatal("flushed before FlushInterval")
		}
		time.Sleep(60 * time.Millisecond)
		synctest.Wait()
		if n := w.flushCount(); n != base+1 {
			t.Fatalf("flushes = %d, want %d", n, base+1)
		}
		close(f.ch("r"))
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestShutdown(t *testing.T) {
	f := newFake()
	s := newServer(t, f, sse.ServerConfig{})
	tk, err := s.Admit(t.Context(), "r")
	if err != nil {
		t.Fatal(err)
	}
	w := newRecorder()
	done := make(chan error, 1)
	go func() { done <- s.Serve(w, request(t.Context(), ""), "r", sse.ServeOptions{Ticket: tk}) }()
	f.send("r", sse.Message{ID: "1-0", Type: sse.TypeToken, Data: []byte("a")})
	for !strings.Contains(w.String(), "data: a") {
		time.Sleep(time.Millisecond)
	}
	if err := s.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, sse.ErrServerClosed) {
		t.Fatalf("Serve = %v, want ErrServerClosed", err)
	}
	if strings.Contains(w.String(), "stream.done") || strings.Contains(w.String(), "stream.cancelled") {
		t.Fatal("shutdown must not send a terminal event")
	}
	if _, err := s.Admit(t.Context(), "r"); !errors.Is(err, sse.ErrServerClosed) {
		t.Fatalf("Admit after shutdown = %v", err)
	}
}

func TestAdmission(t *testing.T) {
	s := newServer(t, newFake(), sse.ServerConfig{MaxConnections: 3, MaxConnectionsPerStream: 2})
	a1, _ := s.Admit(t.Context(), "a")
	a2, _ := s.Admit(t.Context(), "a")
	if _, err := s.Admit(t.Context(), "a"); !errors.Is(err, sse.ErrTooManyForStream) {
		t.Fatalf("third on stream = %v", err)
	}
	b1, _ := s.Admit(t.Context(), "b")
	if _, err := s.Admit(t.Context(), "c"); !errors.Is(err, sse.ErrTooManyConnections) {
		t.Fatalf("over total = %v", err)
	}
	a1.Release()
	a1.Release() // idempotent
	if _, err := s.Admit(t.Context(), "a"); err != nil {
		t.Fatalf("after release = %v", err)
	}
	if err := s.Serve(newRecorder(), request(t.Context(), ""), "other", sse.ServeOptions{Ticket: b1}); err == nil {
		t.Fatal("ticket for another stream accepted")
	}
	a2.Release()
	if _, err := sse.NewServer(newFake(), sse.ServerConfig{MaxConnections: -1}); err == nil {
		t.Fatal("negative limit accepted")
	}
	if _, err := sse.NewServer(nil, sse.ServerConfig{}); err == nil {
		t.Fatal("nil source accepted")
	}
}

func TestServeStreamingUnsupported(t *testing.T) {
	s := newServer(t, newFake(), sse.ServerConfig{})
	var w noFlush
	if err := s.Serve(&w, request(t.Context(), ""), "r", sse.ServeOptions{}); !errors.Is(err, sse.ErrStreamingUnsupported) {
		t.Fatalf("err = %v", err)
	}
}

type noFlush struct{ h http.Header }

func (n *noFlush) Header() http.Header {
	if n.h == nil {
		n.h = http.Header{}
	}
	return n.h
}
func (*noFlush) Write(b []byte) (int, error) { return len(b), nil }
func (*noFlush) WriteHeader(int)             {}
