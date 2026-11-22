package server

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/log"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/i18n"
	"github.com/dungnh3/kitchen/ratelimit"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"golang.org/x/text/language"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestDefaultLogger(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.conn.Close() }) // never Run
	if s.log != log.Default() {
		t.Fatal("without WithLogger the server must use log.Default()")
	}
}

func TestWithI18n(t *testing.T) {
	bundle, err := i18n.New(language.English, i18n.WithDefaults())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(WithLogger(quietLogger()), WithI18n(bundle), WithErrors(errs.NewHandler(errs.Config{}))); err == nil {
		t.Fatal("WithI18n + WithErrors must fail")
	}
	r := start(t, WithServices(healthGateway{}), WithI18n(bundle))
	resp, body := get(t, r.httpURL+"/nope", http.Header{"Accept-Language": {"vi"}})
	e := decodeEnvelope(t, body)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if d := detail(e, "LocalizedMessage"); d == nil || d["locale"] != "vi" {
		t.Fatalf("LocalizedMessage = %v (%s)", d, body)
	}
}

// fakeAuth accepts "Bearer good" as user u-1; "GET /public" is public.
type fakeAuth struct{ patterns *atomic.Value }

func (fakeAuth) check(ctx context.Context, h string) (context.Context, error) {
	if h != "Bearer good" {
		return ctx, errs.ErrTokenMissing
	}
	return xcontext.WithUserID(ctx, "u-1"), nil
}

func (f fakeAuth) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		v := md.Get("authorization")
		if len(v) == 0 {
			v = []string{""}
		}
		ctx, err := f.check(ctx, v[0])
		if err != nil {
			return nil, err
		}
		return h(ctx, req)
	}
}

func (f fakeAuth) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(any, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error {
		return errs.ErrTokenMissing
	}
}

func (f fakeAuth) AuthenticateHTTP(r *http.Request, pattern string) (context.Context, error) {
	f.patterns.Store(pattern)
	if pattern == "GET /public" {
		return r.Context(), nil
	}
	return f.check(r.Context(), r.Header.Get("Authorization"))
}

type authSvc struct{ whoService }

func (a authSvc) RegisterHTTP(m *http.ServeMux) {
	a.whoService.RegisterHTTP(m)
	m.HandleFunc("GET /public", func(http.ResponseWriter, *http.Request) {})
}

func TestWithAuth(t *testing.T) {
	var user, pattern atomic.Value
	r := start(t, WithServices(authSvc{whoService{&user}}, healthGateway{}), WithAuth(fakeAuth{&pattern})) // start waits on gRPC health: public

	// gRPC: no token → 401 envelope from errs; token → user id.
	err := r.conn.Invoke(context.Background(), "/test.Who/Get", &emptypb.Empty{}, &emptypb.Empty{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("no token: %v", err)
	}
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer good")
	if err := r.conn.Invoke(ctx, "/test.Who/Get", &emptypb.Empty{}, &emptypb.Empty{}); err != nil || user.Load() != "u-1" {
		t.Fatalf("token: %v user %v", err, user.Load())
	}

	// Plain HTTP route: fail-closed, by matched pattern, errs envelope.
	resp, body := get(t, r.httpURL+"/whoami", nil)
	if e := decodeEnvelope(t, body); resp.StatusCode != http.StatusUnauthorized || detail(e, "ErrorInfo")["reason"] != "TOKEN_MISSING" ||
		resp.Header.Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("whoami no token: %d %s", resp.StatusCode, body)
	}
	if pattern.Load() != "GET /whoami" {
		t.Fatalf("pattern = %v", pattern.Load())
	}
	user.Store("")
	if resp, _ := get(t, r.httpURL+"/whoami", http.Header{"Authorization": {"Bearer good"}}); resp.StatusCode != http.StatusOK || user.Load() != "u-1" {
		t.Fatalf("whoami token: %d user %v", resp.StatusCode, user.Load())
	}
	if resp, _ := get(t, r.httpURL+"/public", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("public: %d", resp.StatusCode)
	}

	// REST gateway: authenticated once, by the gRPC interceptor.
	if resp, _ := get(t, r.httpURL+"/v1/health", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("gateway health (public gRPC health): %d", resp.StatusCode)
	}
	// Probes are never authenticated.
	if resp, _ := get(t, r.httpURL+"/readyz", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz: %d", resp.StatusCode)
	}
}

// whoGateway maps /test.Who/Get to GET /v1/who like generated gateway code.
type whoGateway struct{ whoService }

func (whoGateway) RegisterGateway(_ context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error {
	return mux.HandlePath(http.MethodGet, "/v1/who", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
		_, outbound := runtime.MarshalerForRequest(mux, r)
		ctx, err := runtime.AnnotateContext(r.Context(), mux, r, "/test.Who/Get", runtime.WithHTTPPathPattern("/v1/who"))
		if err != nil {
			runtime.HTTPError(ctx, mux, outbound, w, r, err)
			return
		}
		var md runtime.ServerMetadata
		resp := new(emptypb.Empty)
		err = conn.Invoke(ctx, "/test.Who/Get", &emptypb.Empty{}, resp, grpc.Header(&md.HeaderMD), grpc.Trailer(&md.TrailerMD))
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outbound, w, r, err)
			return
		}
		runtime.ForwardResponseMessage(ctx, mux, outbound, w, r, resp)
	})
}

func TestWithRateLimit(t *testing.T) {
	lim, err := ratelimit.New(ratelimit.Config{Policies: map[string]ratelimit.Policy{
		"/test.Who/Get": {Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}},
		"GET /whoami":   {Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	var user atomic.Value
	r := start(t, WithServices(whoGateway{whoService{&user}}, healthGateway{}), WithRateLimit(lim))

	// REST through the in-process gateway is keyed by the client IP the
	// server resolved (the in-process peer has none).
	resp, _ := get(t, r.httpURL+"/v1/who", nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("RateLimit-Policy") != `"/test.Who/Get";q=1;w=3600` ||
		resp.Header.Get("X-RateLimit-Remaining") != "0" || resp.Header.Get("Grpc-Metadata-Ratelimit") != "" {
		t.Fatalf("first REST call: %d %v", resp.StatusCode, resp.Header)
	}
	resp, body := get(t, r.httpURL+"/v1/who", nil)
	if e := decodeEnvelope(t, body); resp.StatusCode != http.StatusTooManyRequests || resp.Header.Get("Retry-After") == "" ||
		detail(e, "ErrorInfo")["reason"] != "RATE_LIMITED" {
		t.Fatalf("second REST call: %d %v %s", resp.StatusCode, resp.Header, body)
	}

	// Plain HTTP route, by pattern.
	if resp, _ := get(t, r.httpURL+"/whoami", nil); resp.StatusCode != http.StatusOK || resp.Header.Get("RateLimit") == "" {
		t.Fatalf("first whoami: %d %v", resp.StatusCode, resp.Header)
	}
	if resp, _ := get(t, r.httpURL+"/whoami", nil); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second whoami: %d", resp.StatusCode)
	}

	// Health is never limited.
	for range 3 {
		if resp, _ := get(t, r.httpURL+"/v1/health", nil); resp.StatusCode != http.StatusOK {
			t.Fatalf("health: %d", resp.StatusCode)
		}
	}
}
