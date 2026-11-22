package sse_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/server"
	"github.com/dungnh3/kitchen/sse"
)

// sseSvc mounts an sse.Server on a kitchen server, writing admission errors
// with errs.WriteHTTP.
type sseSvc struct {
	s       *sse.Server
	started chan struct{}
	result  chan error
}

func (*sseSvc) RegisterGRPC(grpc.ServiceRegistrar) {}

func (v *sseSvc) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("GET /streams/{id}", func(w http.ResponseWriter, r *http.Request) {
		t, err := v.s.Admit(r.Context(), r.PathValue("id"))
		if err != nil {
			errs.WriteHTTP(w, r, err)
			return
		}
		close(v.started)
		v.result <- v.s.Serve(w, r, r.PathValue("id"), sse.ServeOptions{Ticket: t})
	})
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestServeEndsWhenKitchenServerDrains(t *testing.T) {
	src := newFake()
	s := newServer(t, src, sse.ServerConfig{Heartbeat: time.Hour})
	svc := &sseSvc{s: s, started: make(chan struct{}), result: make(chan error, 1)}
	hl := listen(t)
	srv, err := server.New(
		server.WithLogger(log.Discard()),
		server.WithGRPCListener(listen(t)), server.WithHTTPListener(hl), server.WithAdminListener(listen(t)),
		server.WithShutdownTimeout(10*time.Second),
		server.WithServices(svc),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	url := "http://" + hl.Addr().String() + "/streams/s1"
	var resp *http.Response
	for range 100 { // wait for the listener to serve
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
		if resp, err = http.DefaultClient.Do(req); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	<-svc.started
	src.send("s1", sse.Message{ID: "1-0", Type: sse.TypeStarted, Data: []byte(`{}`)})
	line, _ := bufio.NewReader(resp.Body).ReadString('\n') // "retry: ..."
	if !strings.HasPrefix(line, "retry:") {
		t.Fatalf("first line %q", line)
	}

	begin := time.Now()
	cancel() // rolling deploy: the kitchen server drains
	select {
	case err := <-svc.result:
		if !errors.Is(err, sse.ErrServerClosed) {
			t.Fatalf("Serve = %v, want ErrServerClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not end on drain")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if d := time.Since(begin); d > 5*time.Second {
		t.Fatalf("shutdown took %v", d)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
}

func TestAdmissionErrorsAreErrs(t *testing.T) {
	s := newServer(t, newFake(), sse.ServerConfig{MaxConnectionsPerStream: 1, MaxConnections: 2})
	ctx := context.Background()
	t1, err := s.Admit(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer t1.Release()
	for _, tt := range []struct {
		stream string
		status int
		reason string
	}{
		{"a", http.StatusTooManyRequests, "CONCURRENCY_LIMITED"},
	} {
		_, err := s.Admit(ctx, tt.stream)
		w := httptest.NewRecorder()
		errs.WriteHTTP(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil), err)
		if w.Code != tt.status || !strings.Contains(w.Body.String(), tt.reason) {
			t.Fatalf("%s: %d %s", tt.stream, w.Code, w.Body.String())
		}
	}
	t2, _ := s.Admit(ctx, "b")
	defer t2.Release()
	_, err = s.Admit(ctx, "c")
	w := httptest.NewRecorder()
	errs.WriteHTTP(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil), err)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") == "" {
		t.Fatalf("server full: %d %v", w.Code, w.Header())
	}
	w = httptest.NewRecorder()
	errs.WriteHTTP(w, httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil), sse.ErrInvalidCursor)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad cursor: %d", w.Code)
	}
}
