package server

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/dungnh3/kitchen/errs"
)

// Review 2026-11-21: an open health Watch stream ends when draining starts,
// so shutdown is fast and Closers get a live context.
func TestWatchEndsOnDrain(t *testing.T) {
	var closed closer
	r := start(t, WithServices(&closed), WithShutdownTimeout(5*time.Second))
	stream, err := healthpb.NewHealthClient(r.conn).Watch(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	begin := time.Now()
	_ = r.stop()
	if d := time.Since(begin); d > 2*time.Second {
		t.Fatalf("shutdown took %s with an open Watch", d)
	}
	if !closed.live.Load() {
		t.Fatal("Close got an expired context")
	}
}

type closer struct {
	live atomic.Bool
}

func (*closer) RegisterGRPC(grpc.ServiceRegistrar) {}

func (c *closer) Close(ctx context.Context) error {
	c.live.Store(ctx.Err() == nil)
	return nil
}

// A panicking or hung readiness check fails readiness; it never crashes the
// process or blocks /readyz forever.
func TestReadinessPanicAndHang(t *testing.T) {
	old := checkTimeout
	checkTimeout = 200 * time.Millisecond
	t.Cleanup(func() { checkTimeout = old })
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	r := start(t, WithReadiness(
		Check{Name: "boom", Func: func(context.Context) error { panic("boom") }},
		Check{Name: "hung", Func: func(context.Context) error { <-release; return nil }},
	))
	resp, _ := get(t, r.httpURL+"/readyz", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("public readyz = %d", resp.StatusCode)
	}
	r.srv.health.mu.Lock()
	r.srv.health.cachedAt = time.Time{}
	r.srv.health.mu.Unlock()
	resp, body := get(t, r.adminURL+"/readyz", nil)
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(body, "panic: boom") || !strings.Contains(body, "timeout") {
		t.Fatalf("admin readyz = %d %s", resp.StatusCode, body)
	}
}

// The user id set on the HTTP side reaches the gRPC handler behind the REST
// gateway (it used to be dropped on the in-process hop).
func TestGatewayForwardsUserID(t *testing.T) {
	type subjectKey struct{}
	var user atomic.Value
	authHTTP := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), subjectKey{}, "u-9")))
		})
	}
	r := start(t, WithServices(whoGateway{whoService{&user}}), WithHTTPMiddlewares(authHTTP),
		WithUserIDFrom(func(ctx context.Context) (string, bool) {
			s, ok := ctx.Value(subjectKey{}).(string)
			return s, ok
		}))
	// A client-sent User-Id must not win over the authenticated one.
	if resp, _ := get(t, r.httpURL+"/v1/who", http.Header{"User-Id": {"admin"}}); resp.StatusCode != http.StatusOK || user.Load() != "u-9" {
		t.Fatalf("gateway user = %v (%d)", user.Load(), resp.StatusCode)
	}
}

// An oversized chunked body is 413 PAYLOAD_TOO_LARGE, like a declared one.
func TestChunkedBodyLimit(t *testing.T) {
	var user atomic.Value
	r := start(t, WithServices(whoGateway{whoService{&user}}, echoGateway{}), WithMaxMessageSize(10))
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, r.httpURL+"/v1/echo",
		struct{ *strings.Reader }{strings.NewReader(strings.Repeat("x", 100))}) // no Content-Length: chunked
	resp, err := noKeepAlive.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

// echoGateway decodes a JSON body the way generated gateway code does.
type echoGateway struct{}

func (echoGateway) RegisterGRPC(grpc.ServiceRegistrar) {}

func (echoGateway) RegisterGateway(_ context.Context, mux *runtime.ServeMux, _ *grpc.ClientConn) error {
	return mux.HandlePath(http.MethodPost, "/v1/echo", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		inbound, outbound := runtime.MarshalerForRequest(mux, r)
		var in structpb.Struct
		if err := inbound.NewDecoder(r.Body).Decode(&in); err != nil {
			runtime.HTTPError(r.Context(), mux, outbound, w, r, status.Errorf(codes.InvalidArgument, "%v", err))
			return
		}
		runtime.ForwardResponseMessage(r.Context(), mux, outbound, w, r, &in)
	})
}

// A body the gateway cannot parse is 400 MALFORMED_REQUEST with the parser's
// message as detail; an InvalidArgument from a service keeps its reason.
func TestMalformedBody(t *testing.T) {
	var user atomic.Value
	r := start(t, WithServices(whoGateway{whoService{&user}}, echoGateway{}))
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, r.httpURL+"/v1/echo", strings.NewReader(`{"a":`))
	resp, err := noKeepAlive.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(b), `"MALFORMED_REQUEST"`) || !strings.Contains(string(b), `"detail"`) {
		t.Fatalf("status = %d body = %s", resp.StatusCode, b)
	}
}

// rejectGateway returns a service-style InvalidArgument (with ErrorInfo).
type rejectGateway struct{}

func (rejectGateway) RegisterGRPC(grpc.ServiceRegistrar) {}

func (rejectGateway) RegisterGateway(_ context.Context, mux *runtime.ServeMux, _ *grpc.ClientConn) error {
	return mux.HandlePath(http.MethodPost, "/v1/reject", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		_, outbound := runtime.MarshalerForRequest(mux, r)
		runtime.HTTPError(r.Context(), mux, outbound, w, r, errs.ErrInvalidPageSize.GRPCStatus().Err())
	})
}

func TestServiceInvalidArgumentKeepsReason(t *testing.T) {
	r := start(t, WithServices(rejectGateway{}))
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, r.httpURL+"/v1/reject", strings.NewReader(`{}`))
	resp, err := noKeepAlive.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(b), `"INVALID_PAGE_SIZE"`) {
		t.Fatalf("status = %d body = %s", resp.StatusCode, b)
	}
}

func TestTruncateValidUTF8(t *testing.T) {
	for _, s := range []string{"abc\xff", strings.Repeat("é", 150)} {
		if got := truncate(s, 200); !utf8.ValidString(got) || len(got) > 200+len("…") {
			t.Errorf("truncate(%q) = %q", s, got)
		}
	}
}

// WithGRPCAddr("") gives an HTTP-only server (group 4: MCP over
// HTTPRegistrar): no gRPC port, the HTTP port with probes and routes works.
func TestHTTPOnly(t *testing.T) {
	if _, err := New(WithLogger(quietLogger()), WithGRPCAddr(""), WithHTTPAddr("")); err == nil {
		t.Fatal("both ports disabled must fail")
	}
	hl := listen(t)
	var user atomic.Value
	srv, err := New(WithLogger(quietLogger()), WithGRPCAddr(""), WithHTTPListener(hl), WithServices(whoService{&user}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	base := "http://" + hl.Addr().String()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, _ := get(t, base+"/readyz", nil)
		if resp.StatusCode == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if resp, _ := get(t, base+"/whoami", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("route: %d", resp.StatusCode)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}
}
