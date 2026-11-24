package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/log"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/i18n"
	"golang.org/x/text/language"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// capture records the request context of health Check calls.
type capture struct {
	mu     sync.Mutex
	userID string
	md     metadata.MD
	info   xcontext.Info
}

func (c *capture) interceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	ri, _ := xcontext.FromContext(ctx)
	c.mu.Lock()
	c.userID, c.md, c.info = xcontext.UserID(ctx), md, ri
	c.mu.Unlock()
	if len(md.Get("x-panic")) > 0 {
		panic("boom")
	}
	if v := md.Get("x-fail"); len(v) > 0 {
		return nil, status.Error(codes.NotFound, "missing")
	}
	return h(ctx, req)
}

func (c *capture) get() (string, metadata.MD, xcontext.Info) {
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
	if resp.Header.Get(xcontext.HeaderRequestID) == "" {
		t.Error("no X-Request-Id in response")
	}

	// Public probes: only /livez and /readyz; other health paths and the
	// version are not served on the public port.
	for path, want := range map[string]int{"/livez": 200, "/readyz": 200, "/health": 404, "/api/status": 404, "/ready": 404, "/version": 404} {
		if resp, body := get(t, r.httpURL+path, nil); resp.StatusCode != want {
			t.Errorf("GET %s = %d %s", path, resp.StatusCode, body)
		}
	}
	for path, want := range map[string]int{"/livez": 200, "/readyz": 200, "/version": 200, "/api/status": 404, "/debug/pprof/": 404} {
		if resp, _ := get(t, r.adminURL+path, nil); resp.StatusCode != want {
			t.Errorf("admin GET %s = %d, want %d", path, resp.StatusCode, want)
		}
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
	defer func() { _ = l.Close() }()
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

	// gRPC: client-sent user-id is removed before the user interceptors.
	ctx := metadata.AppendToOutgoingContext(context.Background(), "user-id", "victim", "x-user-id", "v2", "x-kitchen-client-ip", "1.2.3.4")
	if _, err := healthpb.NewHealthClient(r.conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	uid, md, info := c.get()
	if uid != "" || len(md.Get("user-id")) > 0 || len(md.Get("x-kitchen-client-ip")) > 0 {
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
	proxies, err := xcontext.NewTrustedProxies("127.0.0.0/8")
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

type envelope struct {
	Error struct {
		Code    int              `json:"code"`
		Message string           `json:"message"`
		Status  string           `json:"status"`
		Details []map[string]any `json:"details"`
	} `json:"error"`
}

func decodeEnvelope(t *testing.T, body string) envelope {
	t.Helper()
	var e envelope
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		t.Fatalf("not an AIP-193 envelope: %v: %s", err, body)
	}
	return e
}

func detail(e envelope, typ string) map[string]any {
	for _, d := range e.Error.Details {
		if d["@type"] == "type.googleapis.com/google.rpc."+typ {
			return d
		}
	}
	return nil
}

type panicHTTP struct{}

func (panicHTTP) RegisterGRPC(grpc.ServiceRegistrar) {}
func (panicHTTP) RegisterHTTP(m *http.ServeMux) {
	m.HandleFunc("GET /panic", func(http.ResponseWriter, *http.Request) { panic("boom") })
}

// TestErrorsEverywhere checks that every error the server produces goes
// through the errs.Handler: gateway errors (localized), unknown routes,
// panics, oversized bodies and gRPC errors.
func TestErrorsEverywhere(t *testing.T) {
	bundle, err := i18n.New(language.English, i18n.WithDefaults())
	if err != nil {
		t.Fatal(err)
	}
	c := &capture{}
	var logs syncBuffer
	logger := log.New(&logs, log.Options{Level: log.LevelDebug})
	r := start(t, WithLogger(logger), WithServices(healthGateway{}, panicHTTP{}), WithUnaryInterceptors(c.interceptor),
		WithForwardHeaders("X-Fail"), WithMaxMessageSize(1<<10),
		WithErrors(errs.NewHandler(errs.Config{Logger: logger, Localizer: bundle, Domain: "svc.example.com"})))

	// gRPC handler error over REST, localized.
	resp, body := get(t, r.httpURL+"/v1/health", http.Header{"X-Fail": {"1"}, "Accept-Language": {"vi"}})
	e := decodeEnvelope(t, body)
	if resp.StatusCode != http.StatusNotFound || e.Error.Code != http.StatusNotFound || e.Error.Status != "NOT_FOUND" || e.Error.Message != "missing" {
		t.Fatalf("gateway error = %d %s", resp.StatusCode, body)
	}
	if d := detail(e, "ErrorInfo"); d == nil || d["domain"] != "svc.example.com" || d["reason"] != "NOT_FOUND" {
		t.Errorf("ErrorInfo = %v", d)
	}
	if d := detail(e, "LocalizedMessage"); d == nil || d["locale"] != "vi" {
		t.Errorf("LocalizedMessage = %v", d)
	}

	// Unknown route.
	resp, body = get(t, r.httpURL+"/nope", nil)
	if e := decodeEnvelope(t, body); resp.StatusCode != http.StatusNotFound || e.Error.Status != "NOT_FOUND" {
		t.Errorf("unknown route = %d %s", resp.StatusCode, body)
	}

	// Panic in a plain HTTP handler: 500 envelope and an access log line.
	resp, body = get(t, r.httpURL+"/panic", nil)
	if e := decodeEnvelope(t, body); resp.StatusCode != http.StatusInternalServerError || e.Error.Status != "INTERNAL" || strings.Contains(body, "boom") {
		t.Errorf("panic = %d %s", resp.StatusCode, body)
	}
	if out := logs.String(); !strings.Contains(out, `"url.path":"/panic"`) || !strings.Contains(out, `"http.response.status_code":500`) {
		t.Errorf("panicking request not access-logged: %s", out)
	}

	// Oversized body.
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, r.httpURL+"/v1/health", strings.NewReader(strings.Repeat("a", 2<<10)))
	presp, perr := http.DefaultClient.Do(req)
	if perr != nil {
		t.Fatal(perr)
	}
	pbody, _ := io.ReadAll(presp.Body)
	_ = presp.Body.Close()
	if e := decodeEnvelope(t, string(pbody)); presp.StatusCode != http.StatusRequestEntityTooLarge || detail(e, "ErrorInfo")["reason"] != "PAYLOAD_TOO_LARGE" {
		t.Errorf("oversized body = %d %s", presp.StatusCode, pbody)
	}

	// gRPC: same conversion, panics included.
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-fail", "1")
	_, err = healthpb.NewHealthClient(r.conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if info, ok := errs.Info(err); !ok || info.GetDomain() != "svc.example.com" {
		t.Errorf("gRPC error = %v", err)
	}
	ctx = metadata.AppendToOutgoingContext(context.Background(), "x-panic", "1")
	_, err = healthpb.NewHealthClient(r.conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if info, ok := errs.Info(err); !ok || status.Code(err) != codes.Internal || info.GetReason() != "INTERNAL" {
		t.Errorf("gRPC panic = %v", err)
	}
}

// whoService is a minimal gRPC service whose handler records the user id.
type whoService struct{ user *atomic.Value }

func (w whoService) RegisterGRPC(r grpc.ServiceRegistrar) {
	r.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Who",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Get",
			Handler: func(_ any, ctx context.Context, dec func(any) error, ic grpc.UnaryServerInterceptor) (any, error) {
				in := new(emptypb.Empty)
				if err := dec(in); err != nil {
					return nil, err
				}
				h := func(ctx context.Context, _ any) (any, error) {
					w.user.Store(xcontext.UserID(ctx))
					return &emptypb.Empty{}, nil
				}
				if ic == nil {
					return h(ctx, in)
				}
				return ic(ctx, in, &grpc.UnaryServerInfo{FullMethod: "/test.Who/Get"}, h)
			},
		}},
	}, w)
}

func (w whoService) RegisterHTTP(m *http.ServeMux) {
	m.HandleFunc("GET /whoami", func(_ http.ResponseWriter, r *http.Request) {
		w.user.Store(xcontext.UserID(r.Context()))
	})
}

func TestUserIDFrom(t *testing.T) {
	type subjectKey struct{}
	authUnary := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		return h(context.WithValue(ctx, subjectKey{}, "u-42"), req)
	}
	authHTTP := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), subjectKey{}, "u-7")))
		})
	}
	var user atomic.Value
	r := start(t, WithServices(whoService{&user}), WithUnaryInterceptors(authUnary), WithHTTPMiddlewares(authHTTP),
		WithUserIDFrom(func(ctx context.Context) (string, bool) {
			s, ok := ctx.Value(subjectKey{}).(string)
			return s, ok
		}))
	if err := r.conn.Invoke(context.Background(), "/test.Who/Get", &emptypb.Empty{}, &emptypb.Empty{}); err != nil {
		t.Fatal(err)
	}
	if user.Load() != "u-42" {
		t.Errorf("gRPC user = %v", user.Load())
	}
	if resp, _ := get(t, r.httpURL+"/whoami", nil); resp.StatusCode != http.StatusOK || user.Load() != "u-7" {
		t.Errorf("HTTP user = %v", user.Load())
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
	resp, body = get(t, r.httpURL+"/readyz", nil)
	if resp.StatusCode != http.StatusServiceUnavailable || strings.Contains(body, "db down") {
		t.Fatalf("public /readyz = %d %s (must not leak details)", resp.StatusCode, body)
	}
	fail.Store(false)
	if resp, _ := get(t, r.adminURL+"/readyz", nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("result must be cached for %v, got %d", checkTTL, resp.StatusCode)
	}
	r.srv.health.mu.Lock()
	r.srv.health.cachedAt = r.srv.health.cachedAt.Add(-checkTTL) // expire the cache
	r.srv.health.mu.Unlock()
	if resp, _ := get(t, r.adminURL+"/readyz", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz after recovery = %d", resp.StatusCode)
	}
}

func TestReadinessSharedRun(t *testing.T) {
	var runs atomic.Int64
	release := make(chan struct{})
	r := start(t, WithReadiness(Check{Name: "slow", Func: func(context.Context) error {
		runs.Add(1)
		<-release
		return nil
	}}))
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			if resp, err := http.Get(r.httpURL + "/readyz"); err == nil {
				_ = resp.Body.Close()
			}
		})
	}
	time.Sleep(200 * time.Millisecond) // let the probes pile up on the in-flight run
	close(release)
	wg.Wait()
	for range 50 {
		if resp, err := http.Get(r.httpURL + "/readyz"); err == nil {
			_ = resp.Body.Close()
		}
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("150 probes ran the check %d times, want 1", n)
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
	_ = resp.Body.Close()
	// PAYLOAD_TOO_LARGE is INVALID_ARGUMENT with HTTP 413 (an errs reason
	// exception to the code-to-status mapping).
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
	defer func() { _ = resp.Body.Close() }()
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
	// The Watch stream ends when draining starts; Run must return.
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
	defer func() { _ = resp.Body.Close() }()
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

func TestIdentityStrippedIsLogged(t *testing.T) {
	var buf syncBuffer
	logger := log.New(&buf, log.Options{Level: log.LevelWarn})
	r := start(t, WithServices(healthGateway{}), WithLogger(logger))
	ctx := metadata.AppendToOutgoingContext(context.Background(), "user-id", "u1")
	for range 3 {
		if _, err := healthpb.NewHealthClient(r.conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	out := buf.String()
	if n := strings.Count(out, "dropped identity metadata"); n != 1 {
		t.Fatalf("want exactly one rate-limited warning, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, `"key":"User-Id"`) || !strings.Contains(out, `"transport":"grpc"`) {
		t.Errorf("warning lacks key/transport: %s", out)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// grpc.health.v1 follows the readiness checks, like /readyz.
func TestGRPCHealthFollowsReadiness(t *testing.T) {
	var down atomic.Bool
	r := start(t, WithReadiness(Check{Name: "db", Func: func(context.Context) error {
		if down.Load() {
			return errors.New("db down")
		}
		return nil
	}}))
	hc := healthpb.NewHealthClient(r.conn)
	waitStatus := func(want healthpb.HealthCheckResponse_ServingStatus) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			resp, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{})
			if err == nil && resp.GetStatus() == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("grpc health = %v (%v), want %v", resp.GetStatus(), err, want)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	down.Store(true)
	waitStatus(healthpb.HealthCheckResponse_NOT_SERVING)
	down.Store(false)
	waitStatus(healthpb.HealthCheckResponse_SERVING)
}
