package server

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/reqctx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// capture records the request context of health Check calls.
type capture struct {
	mu     sync.Mutex
	userID string
	md     metadata.MD
	info   reqctx.Info
}

func (c *capture) interceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	ri, _ := reqctx.FromContext(ctx)
	c.mu.Lock()
	c.userID, c.md, c.info = reqctx.UserID(ctx), md, ri
	c.mu.Unlock()
	if len(md.Get("x-panic")) > 0 {
		panic("boom")
	}
	if v := md.Get("x-fail"); len(v) > 0 {
		return nil, status.Error(codes.NotFound, "missing")
	}
	return h(ctx, req)
}

func (c *capture) get() (string, metadata.MD, reqctx.Info) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.userID, c.md, c.info
}

func TestRESTAndGRPC(t *testing.T) {
	r := start(t, WithServices(healthGateway{}))

	resp, body := get(t, r.httpURL+"/v1/health", nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, `"status":"SERVING"`) {
		t.Fatalf("REST health = %d %s", resp.StatusCode, body)
	}
	if resp.Header.Get(reqctx.HeaderRequestID) == "" {
		t.Error("no X-Request-Id in response")
	}

	for path, want := range map[string]int{"/health": 200, "/api/status": 200, "/ready": 200, "/version": 200} {
		if resp, body := get(t, r.httpURL+path, nil); resp.StatusCode != want {
			t.Errorf("GET %s = %d %s", path, resp.StatusCode, body)
		}
	}
	for path, want := range map[string]int{"/livez": 200, "/readyz": 200, "/debug/pprof/": 404} {
		if resp, _ := get(t, r.adminURL+path, nil); resp.StatusCode != want {
			t.Errorf("admin GET %s = %d, want %d", path, resp.StatusCode, want)
		}
	}
	if resp, _ := get(t, r.httpURL+"/livez", nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("/livez must not be on the public port, got %d", resp.StatusCode)
	}
	if err := r.stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
}

func TestRunTwice(t *testing.T) {
	r := start(t)
	if err := r.srv.Run(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("second Run = %v", err)
	}
}

func TestListenError(t *testing.T) {
	l := listen(t)
	defer l.Close()
	srv, err := New(WithLogger(quietLogger()), WithGRPCAddr(l.Addr().String()), WithHTTPAddr(""))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Run(context.Background()); err == nil {
		t.Fatal("Run on a used port succeeded")
	}
}

func TestIdentityStripped(t *testing.T) {
	c := &capture{}
	r := start(t, WithServices(healthGateway{}), WithUnaryInterceptors(c.interceptor))

	// gRPC: client-sent user-id is removed before any interceptor.
	ctx := metadata.AppendToOutgoingContext(context.Background(), "user-id", "victim", "x-user-id", "v2", "x-kitchen-client-ip", "1.2.3.4")
	if _, err := healthpb.NewHealthClient(r.conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	uid, md, info := c.get()
	if uid != "" || len(md.Get("user-id")) > 0 || len(md.Get("x-user-id")) > 0 {
		t.Fatalf("gRPC identity not stripped: uid=%q md=%v", uid, md)
	}
	if info.ClientIP.String() != "127.0.0.1" {
		t.Errorf("gRPC client ip = %v", info.ClientIP)
	}
	if info.RequestID == "" {
		t.Error("no request id generated")
	}

	// REST: User-Id and Grpc-Metadata-* are removed.
	h := http.Header{}
	h.Set("User-Id", "victim")
	h.Set("Grpc-Metadata-User-Id", "victim")
	h.Set("X-Device-Id", "dev-1")
	h.Set("X-Request-Id", "req-123")
	resp, body := get(t, r.httpURL+"/v1/health", h)
	if resp.StatusCode != http.StatusOK {
		t.Fatal(body)
	}
	uid, md, info = c.get()
	if uid != "" || len(md.Get("user-id")) > 0 {
		t.Fatalf("REST identity not stripped: uid=%q md=%v", uid, md)
	}
	if info.DeviceID != "dev-1" || info.RequestID != "req-123" || info.ClientIP.String() != "127.0.0.1" {
		t.Errorf("REST info = %+v", info)
	}
	if resp.Header.Get("X-Request-Id") != "req-123" {
		t.Errorf("response request id = %q", resp.Header.Get("X-Request-Id"))
	}
}

func TestIdentityTrusted(t *testing.T) {
	c := &capture{}
	proxies, err := reqctx.NewTrustedProxies("127.0.0.0/8")
	if err != nil {
		t.Fatal(err)
	}
	r := start(t, WithServices(healthGateway{}), WithUnaryInterceptors(c.interceptor),
		WithTrustedIdentityPeers(proxies), WithTrustedProxies(proxies))

	ctx := metadata.AppendToOutgoingContext(context.Background(), "user-id", "u1", "x-forwarded-for", "9.9.9.9")
	if _, err := healthpb.NewHealthClient(r.conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	if uid, _, info := c.get(); uid != "u1" || info.ClientIP.String() != "9.9.9.9" {
		t.Fatalf("trusted gRPC: uid=%q ip=%v", uid, info.ClientIP)
	}

	h := http.Header{}
	h.Set("User-Id", "u2")
	h.Set("X-Forwarded-For", "8.8.8.8, 127.0.0.1")
	if resp, body := get(t, r.httpURL+"/v1/health", h); resp.StatusCode != http.StatusOK {
		t.Fatal(body)
	}
	if uid, _, info := c.get(); uid != "u2" || info.ClientIP.String() != "8.8.8.8" {
		t.Fatalf("trusted REST: uid=%q ip=%v", uid, info.ClientIP)
	}
}

func TestRecovery(t *testing.T) {
	c := &capture{}
	r := start(t, WithUnaryInterceptors(c.interceptor))
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-panic", "1")
	_, err := healthpb.NewHealthClient(r.conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if status.Code(err) != codes.Internal {
		t.Fatalf("panic = %v, want Internal", err)
	}
	if _, err := healthpb.NewHealthClient(r.conn).Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("server unusable after panic: %v", err)
	}
}

func TestHTTPStatusHook(t *testing.T) {
	c := &capture{}
	r := start(t, WithServices(healthGateway{}), WithUnaryInterceptors(c.interceptor),
		WithForwardHeaders("X-Fail"),
		WithHTTPStatus(func(st *status.Status) int {
			if st.Code() == codes.NotFound {
				return http.StatusGone
			}
			return 0
		}))
	resp, body := get(t, r.httpURL+"/v1/health", http.Header{"X-Fail": {"1"}})
	if resp.StatusCode != http.StatusGone || !strings.Contains(body, `"message":"missing"`) || !strings.Contains(body, `"code":5`) {
		t.Fatalf("got %d %s", resp.StatusCode, body)
	}
}

func TestReadinessChecks(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	r := start(t, WithReadiness(Check{Name: "db", Func: func(context.Context) error {
		if fail.Load() {
			return errors.New("db down")
		}
		return nil
	}}))
	resp, body := get(t, r.adminURL+"/readyz", nil)
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(body, "db down") {
		t.Fatalf("admin readyz = %d %s", resp.StatusCode, body)
	}
	resp, body = get(t, r.httpURL+"/ready", nil)
	if resp.StatusCode != http.StatusServiceUnavailable || strings.Contains(body, "db down") {
		t.Fatalf("public /ready = %d %s (must not leak details)", resp.StatusCode, body)
	}
	fail.Store(false)
	if resp, _ := get(t, r.adminURL+"/readyz", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz after recovery = %d", resp.StatusCode)
	}
}

func TestCORS(t *testing.T) {
	r := start(t, WithServices(healthGateway{}))
	resp, _ := get(t, r.httpURL+"/v1/health", http.Header{"Origin": {"https://evil.example"}})
	if v := resp.Header.Get("Access-Control-Allow-Origin"); v != "" {
		t.Fatalf("CORS on by default: %q", v)
	}

	r2 := start(t, WithServices(healthGateway{}), WithCORS(CORSConfig{AllowedOrigins: []string{"https://app.example"}}))
	resp, _ = get(t, r2.httpURL+"/v1/health", http.Header{"Origin": {"https://app.example"}})
	if v := resp.Header.Get("Access-Control-Allow-Origin"); v != "https://app.example" {
		t.Fatalf("allowed origin = %q", v)
	}
	resp, _ = get(t, r2.httpURL+"/v1/health", http.Header{"Origin": {"https://evil.example"}})
	if v := resp.Header.Get("Access-Control-Allow-Origin"); v != "" {
		t.Fatalf("disallowed origin got %q", v)
	}
}

func TestBodyLimit(t *testing.T) {
	r := start(t, WithServices(healthGateway{}), WithMaxMessageSize(10))
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, r.httpURL+"/v1/health", strings.NewReader(strings.Repeat("x", 100)))
	resp, err := noKeepAlive.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestStreamingSSE(t *testing.T) {
	r := start(t, WithServices(healthGateway{}), WithStreamingMarshalers(), WithShutdownTimeout(time.Second))
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, r.httpURL+"/v1/health/watch", nil)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := noKeepAlive.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if line != "data: {\"status\":\"SERVING\"}\n" {
		t.Fatalf("first event %q", line)
	}
	// Watch ignores Draining, so shutdown force-closes it at the deadline;
	// Run must still return.
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
}

func TestStreamingNDJSON(t *testing.T) {
	m := &ndjsonMarshaler{JSONPb: *JSONMarshaler()}
	b, err := m.Marshal(map[string]any{"result": &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}})
	if err != nil || string(b) != `{"status":"SERVING"}` {
		t.Fatalf("ndjson = %s %v", b, err)
	}
	s := &sseMarshaler{JSONPb: *JSONMarshaler()}
	b, err = s.Marshal(map[string]proto.Message{"error": status.New(codes.Internal, "x").Proto()})
	if err != nil || !strings.HasPrefix(string(b), "event: error\ndata: {") {
		t.Fatalf("sse error = %q %v", b, err)
	}
}

// drainSvc is an HTTP long-poll that ends on Draining, and records Close.
type drainSvc struct {
	closed  atomic.Bool
	drained atomic.Bool
	started chan struct{}
}

func (*drainSvc) RegisterGRPC(grpc.ServiceRegistrar) {}

func (d *drainSvc) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("GET /poll", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = http.NewResponseController(w).Flush()
		close(d.started)
		select {
		case <-Draining(r.Context()):
			d.drained.Store(true)
			_, _ = w.Write([]byte("bye"))
		case <-r.Context().Done():
		}
	})
}

func (d *drainSvc) Close(context.Context) error {
	if !d.drained.Load() {
		return errors.New("closed before drain")
	}
	d.closed.Store(true)
	return nil
}

func TestDrainThenClose(t *testing.T) {
	svc := &drainSvc{started: make(chan struct{})}
	r := start(t, WithServices(svc))
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, r.httpURL+"/poll", nil)
	resp, err := noKeepAlive.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	<-svc.started
	begin := time.Now()
	if err := r.stop(); err != nil {
		t.Fatal(err)
	}
	if time.Since(begin) > 3*time.Second {
		t.Errorf("shutdown took %v", time.Since(begin))
	}
	b, _ := bufio.NewReader(resp.Body).ReadString(0)
	if b != "bye" || !svc.closed.Load() {
		t.Fatalf("body %q closed=%v", b, svc.closed.Load())
	}
	if !r.srv.ShuttingDown() {
		t.Error("ShuttingDown false after Run")
	}
}
