package grpcx_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/grpcx"
	"github.com/dungnh3/kitchen/grpcx/grpcxtest"
	"github.com/dungnh3/kitchen/log"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// fakeHealth answers Check with check and Watch with watch, and records the
// incoming metadata of the last call.
type fakeHealth struct {
	healthpb.UnimplementedHealthServer
	calls atomic.Int32
	check func(ctx context.Context, n int32) error
	watch func(n int32, s grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error

	mu       sync.Mutex
	md       metadata.MD
	deadline time.Duration
	hasDl    bool
}

func (f *fakeHealth) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	n := f.calls.Add(1)
	md, _ := metadata.FromIncomingContext(ctx)
	dl, ok := ctx.Deadline()
	f.mu.Lock()
	f.md, f.hasDl = md, ok
	if ok {
		f.deadline = time.Until(dl)
	}
	f.mu.Unlock()
	if f.check != nil {
		if err := f.check(ctx, n); err != nil {
			return nil, err
		}
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (f *fakeHealth) Watch(_ *healthpb.HealthCheckRequest, s grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error {
	n := f.calls.Add(1)
	if f.watch != nil {
		return f.watch(n, s)
	}
	return s.Send(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING})
}

func (f *fakeHealth) lastMD() metadata.MD {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.md
}

func serve(t *testing.T, f *fakeHealth, tlsServer bool, opts ...grpcx.Option) healthpb.HealthClient {
	t.Helper()
	register := func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, f) }
	srv := grpcxtest.NewServer(t, register)
	if tlsServer {
		srv = grpcxtest.NewTLSServer(t, register)
	}
	return healthpb.NewHealthClient(srv.Conn(t, append([]grpcx.Option{grpcx.WithLogger(log.Discard())}, opts...)...))
}

func TestNewRefuses(t *testing.T) {
	tok := grpcx.StaticToken("t")
	tests := []struct {
		name   string
		target string
		opts   []grpcx.Option
		want   string
	}{
		{"empty target", " ", nil, "empty target"},
		{"insecure and tls", "x:1", []grpcx.Option{grpcx.WithInsecure(), grpcx.WithTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})}, "WithTLSConfig"},
		{"token over plaintext", "x:1", []grpcx.Option{grpcx.WithInsecure(), grpcx.WithTokenSource(tok)}, "require TLS"},
		{"forward over plaintext", "x:1", []grpcx.Option{grpcx.WithInsecure(), grpcx.WithForwardAuthorization()}, "require TLS"},
		{"token and forward", "x:1", []grpcx.Option{grpcx.WithTokenSource(tok), grpcx.WithForwardAuthorization()}, "exclusive"},
		{"negative timeout", "x:1", []grpcx.Option{grpcx.WithTimeout(-time.Second)}, "negative"},
		{"one attempt", "x:1", []grpcx.Option{grpcx.WithRetry(grpcx.RetryPolicy{MaxAttempts: 1})}, "MaxAttempts"},
		{"retry OK", "x:1", []grpcx.Option{grpcx.WithRetry(grpcx.RetryPolicy{Codes: []codes.Code{codes.OK}})}, "OK"},
		{"unknown balancer", "x:1", []grpcx.Option{grpcx.WithLoadBalancing("nope")}, "grpcx"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := grpcx.New(tt.target, tt.opts...)
			if err == nil {
				_ = c.Close(t.Context())
				t.Fatal("New succeeded")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err %q lacks %q", err, tt.want)
			}
		})
	}
}

func TestNewDefaultsTLS(t *testing.T) {
	c, err := grpcx.New("dns:///billing.internal:443")
	if err != nil {
		t.Fatal(err)
	}
	if c.Target() != "dns:///billing.internal:443" {
		t.Errorf("target %q", c.Target())
	}
	if err := c.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A plaintext server refuses the TLS handshake of a default Conn.
	f := &fakeHealth{}
	srv := grpcxtest.NewServer(t, func(r grpc.ServiceRegistrar) { healthpb.RegisterHealthServer(r, f) })
	tlsConn, err := grpcx.New("passthrough:///bufnet", grpcx.WithDialOptions(srv.DialOption()), grpcx.WithoutRetry())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tlsConn.Close(t.Context()) }()
	_, err = healthpb.NewHealthClient(tlsConn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	if !errors.Is(err, errs.ErrDependencyUnavailable) || f.calls.Load() != 0 {
		t.Errorf("TLS client to plaintext server: err %v, calls %d", err, f.calls.Load())
	}
}

func TestErrorMapping(t *testing.T) {
	retryInfo := func() error {
		st, _ := status.New(codes.Unavailable, "secret downstream detail").WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(3 * time.Second)})
		return st.Err()
	}
	tests := []struct {
		name     string
		err      error
		want     *errs.Error
		wantCode codes.Code
	}{
		{"ok", nil, nil, codes.OK},
		{"not found", status.Error(codes.NotFound, "secret downstream detail"), errs.ErrNotFound, codes.NotFound},
		{"deadline", status.Error(codes.DeadlineExceeded, "secret downstream detail"), errs.ErrUpstreamTimeout, codes.DeadlineExceeded},
		{"unavailable", retryInfo(), errs.ErrDependencyUnavailable, codes.Unavailable},
		{"exhausted", status.Error(codes.ResourceExhausted, "secret downstream detail"), errs.ErrDependencyUnavailable, codes.ResourceExhausted},
		{"invalid argument", status.Error(codes.InvalidArgument, "secret downstream detail"), errs.ErrInternal, codes.InvalidArgument},
		{"permission denied", status.Error(codes.PermissionDenied, "secret downstream detail"), errs.ErrInternal, codes.PermissionDenied},
		{"unauthenticated", status.Error(codes.Unauthenticated, "secret downstream detail"), errs.ErrInternal, codes.Unauthenticated},
		{"internal", status.Error(codes.Internal, "secret downstream detail"), errs.ErrInternal, codes.Internal},
		{"plain error", errors.New("secret downstream detail"), errs.ErrInternal, codes.Unknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeHealth{check: func(context.Context, int32) error { return tt.err }}
			c := serve(t, f, false, grpcx.WithoutRetry())
			_, err := c.Check(t.Context(), &healthpb.HealthCheckRequest{})
			if tt.want == nil {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, tt.want) || errs.Code(err) != tt.want.Code() {
				t.Fatalf("err %v (code %v), want %v", err, errs.Code(err), tt.want)
			}
			if got := grpcx.Code(err); got != tt.wantCode {
				t.Errorf("grpcx.Code = %v, want %v", got, tt.wantCode)
			}
			if se, ok := errors.AsType[*grpcx.StatusError](err); !ok || se.Method != "/grpc.health.v1.Health/Check" {
				t.Errorf("StatusError %+v", se)
			}
			// What a kitchen server would send to its own clients.
			sent := status.Convert(err)
			if strings.Contains(sent.Message(), "secret") {
				t.Errorf("downstream message leaked: %q", sent.Message())
			}
			if tt.name == "unavailable" {
				var delay time.Duration
				for _, d := range sent.Details() {
					if ri, ok := d.(*errdetails.RetryInfo); ok {
						delay = ri.GetRetryDelay().AsDuration()
					}
				}
				if delay != 3*time.Second {
					t.Errorf("RetryInfo delay %v, want 3s", delay)
				}
			}
		})
	}
}

func TestCustomErrorMapper(t *testing.T) {
	errDeclined := errs.New(codes.FailedPrecondition, "CARD_DECLINED")
	f := &fakeHealth{check: func(context.Context, int32) error {
		return errs.New(codes.FailedPrecondition, "CARD_DECLINED", errs.Domain("billing")).GRPCStatus().Err()
	}}
	c := serve(t, f, false, grpcx.WithErrorMapper(func(se *grpcx.StatusError) error {
		if errors.Is(errs.FromStatus(se.Status), errDeclined) {
			return errs.Wrap(se, errDeclined)
		}
		return grpcx.DefaultErrorMapper(se)
	}))
	_, err := c.Check(t.Context(), &healthpb.HealthCheckRequest{})
	if !errors.Is(err, errDeclined) {
		t.Errorf("err %v, want CARD_DECLINED", err)
	}

	raw := serve(t, f, false, grpcx.WithErrorMapper(nil))
	_, err = raw.Check(t.Context(), &healthpb.HealthCheckRequest{})
	if se, ok := err.(*grpcx.StatusError); !ok || errs.Code(se) != codes.Unknown { //nolint:errorlint // the bare StatusError
		t.Errorf("nil mapper: err %T %v; errs.Code %v (a bare StatusError must not carry the downstream status)", err, err, errs.Code(err))
	}
}

func TestRetry(t *testing.T) {
	fast := grpcx.RetryPolicy{InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
	tests := []struct {
		name      string
		opts      []grpcx.Option
		code      codes.Code
		failures  int32
		wantCalls int32
		wantErr   bool
	}{
		{"unavailable retried", []grpcx.Option{grpcx.WithRetry(fast)}, codes.Unavailable, 2, 3, false},
		{"gives up after max attempts", []grpcx.Option{grpcx.WithRetry(fast)}, codes.Unavailable, 5, 3, true},
		{"internal not retried", []grpcx.Option{grpcx.WithRetry(fast)}, codes.Internal, 1, 1, true},
		{"extra code retried", []grpcx.Option{grpcx.WithRetry(grpcx.RetryPolicy{InitialBackoff: time.Millisecond, Codes: []codes.Code{codes.Aborted}})}, codes.Aborted, 1, 2, false},
		{"without retry", []grpcx.Option{grpcx.WithoutRetry()}, codes.Unavailable, 1, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeHealth{check: func(_ context.Context, n int32) error {
				if n <= tt.failures {
					return status.Error(tt.code, "try again")
				}
				return nil
			}}
			c := serve(t, f, false, tt.opts...)
			_, err := c.Check(t.Context(), &healthpb.HealthCheckRequest{})
			if (err != nil) != tt.wantErr || f.calls.Load() != tt.wantCalls {
				t.Errorf("err %v, calls %d; want error %v, %d calls", err, f.calls.Load(), tt.wantErr, tt.wantCalls)
			}
		})
	}
}

func TestDefaultDeadline(t *testing.T) {
	tests := []struct {
		name    string
		opts    []grpcx.Option
		ctxDl   time.Duration
		wantMax time.Duration
		wantDl  bool
	}{
		{"default", nil, 0, grpcx.DefaultTimeout, true},
		{"custom", []grpcx.Option{grpcx.WithTimeout(2 * time.Second)}, 0, 2 * time.Second, true},
		{"caller deadline wins", nil, time.Hour, time.Hour, true},
		{"disabled", []grpcx.Option{grpcx.WithTimeout(0)}, 0, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeHealth{}
			c := serve(t, f, false, tt.opts...)
			ctx := t.Context()
			if tt.ctxDl > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.ctxDl)
				defer cancel()
			}
			if _, err := c.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.hasDl != tt.wantDl || tt.wantDl && (f.deadline > tt.wantMax || f.deadline < tt.wantMax-time.Minute/2) {
				t.Errorf("deadline %v (set %v), want about %v (set %v)", f.deadline, f.hasDl, tt.wantMax, tt.wantDl)
			}
		})
	}
}

func TestPropagation(t *testing.T) {
	f := &fakeHealth{}
	c := serve(t, f, false)
	ctx := xcontext.NewContext(t.Context(), xcontext.Info{RequestID: "req-1"})
	ctx = xcontext.WithUserID(ctx, "user-1")
	ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", "Bearer incoming", "x-secret", "s"))
	if _, err := c.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	md := f.lastMD()
	if got := md.Get("x-request-id"); len(got) != 1 || got[0] != "req-1" {
		t.Errorf("x-request-id %v", got)
	}
	if got := md.Get("user-id"); len(got) != 1 || got[0] != "user-1" {
		t.Errorf("user-id %v", got)
	}
	if len(md.Get("authorization")) != 0 || len(md.Get("x-secret")) != 0 {
		t.Errorf("incoming metadata forwarded without WithForwardAuthorization: %v", md)
	}
	if ua := md.Get("user-agent"); len(ua) == 0 || !strings.HasPrefix(ua[0], "kitchen-grpcx") {
		t.Errorf("user-agent %v", ua)
	}
}

func TestTokenSource(t *testing.T) {
	tests := []struct {
		name     string
		ts       grpcx.TokenSource
		wantAuth string
		wantCode codes.Code
	}{
		{"static", grpcx.StaticToken("tok-1"), "Bearer tok-1", codes.OK},
		{"func", grpcx.TokenSourceFunc(func(context.Context) (string, error) { return "tok-2", nil }), "Bearer tok-2", codes.OK},
		{"error", grpcx.TokenSourceFunc(func(context.Context) (string, error) { return "", errors.New("idp down") }), "", codes.Unauthenticated},
		{"empty", grpcx.StaticToken(""), "", codes.Unauthenticated},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeHealth{}
			c := serve(t, f, true, grpcx.WithTokenSource(tt.ts))
			_, err := c.Check(t.Context(), &healthpb.HealthCheckRequest{})
			if grpcx.Code(err) != tt.wantCode {
				t.Fatalf("err %v, want code %v", err, tt.wantCode)
			}
			if err != nil {
				if !errors.Is(err, errs.ErrInternal) || f.calls.Load() != 0 {
					t.Errorf("token failure: err %v, calls %d; want Internal and no call", err, f.calls.Load())
				}
				return
			}
			if got := f.lastMD().Get("authorization"); len(got) != 1 || got[0] != tt.wantAuth {
				t.Errorf("authorization %v, want %q", got, tt.wantAuth)
			}
		})
	}
}

func TestForwardAuthorization(t *testing.T) {
	tests := []struct {
		name     string
		incoming []string
		outgoing string
		want     []string
	}{
		{"forwarded", []string{"Bearer abc"}, "", []string{"Bearer abc"}},
		{"scheme case", []string{"bearer abc"}, "", []string{"Bearer abc"}},
		{"explicit outgoing wins", []string{"Bearer abc"}, "Bearer mine", []string{"Bearer mine"}},
		{"basic not forwarded", []string{"Basic dTpw"}, "", nil},
		{"two values not forwarded", []string{"Bearer a", "Bearer b"}, "", nil},
		{"malformed not forwarded", []string{"Bearer a b"}, "", nil},
		{"none", nil, "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeHealth{}
			c := serve(t, f, true, grpcx.WithForwardAuthorization())
			md := metadata.MD{}
			for _, v := range tt.incoming {
				md.Append("authorization", v)
			}
			ctx := metadata.NewIncomingContext(t.Context(), md)
			if tt.outgoing != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", tt.outgoing)
			}
			if _, err := c.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
				t.Fatal(err)
			}
			if got := f.lastMD().Get("authorization"); strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("authorization %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStreams(t *testing.T) {
	tests := []struct {
		name  string
		watch func(int32, grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error
		want  error
	}{
		{"end of stream", nil, io.EOF},
		{"downstream error", func(_ int32, s grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error {
			_ = s.Send(&healthpb.HealthCheckResponse{})
			return status.Error(codes.NotFound, "secret")
		}, errs.ErrNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeHealth{watch: tt.watch}
			c := serve(t, f, false, grpcx.WithCircuitBreaker(grpcx.BreakerConfig{}))
			s, err := c.Watch(t.Context(), &healthpb.HealthCheckRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Recv(); err != nil {
				t.Fatal(err)
			}
			_, err = s.Recv()
			if !errors.Is(err, tt.want) {
				t.Errorf("err %v, want %v", err, tt.want)
			}
		})
	}
}

func TestCanceled(t *testing.T) {
	f := &fakeHealth{}
	c := serve(t, f, false, grpcx.WithCircuitBreaker(grpcx.BreakerConfig{FailureThreshold: 1}))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for range 3 {
		_, err := c.Check(ctx, &healthpb.HealthCheckRequest{})
		if !errors.Is(err, context.Canceled) || !errors.Is(err, errs.ErrCanceled) {
			t.Fatalf("err %v, want context.Canceled and errs.ErrCanceled", err)
		}
	}
	// Cancelled calls do not open the breaker.
	if _, err := c.Check(t.Context(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Errorf("after cancelled calls: %v", err)
	}
}

func TestBreakerOpens(t *testing.T) {
	var changes []string
	var mu sync.Mutex
	f := &fakeHealth{check: func(_ context.Context, n int32) error {
		if n == 1 {
			return status.Error(codes.NotFound, "no") // not a failure
		}
		return status.Error(codes.Unavailable, "down")
	}}
	c := serve(t, f, false, grpcx.WithoutRetry(), grpcx.WithCircuitBreaker(grpcx.BreakerConfig{
		FailureThreshold: 2,
		OnStateChange: func(from, to string) {
			mu.Lock()
			defer mu.Unlock()
			changes = append(changes, from+">"+to)
		},
	}))
	for range 3 {
		_, _ = c.Check(t.Context(), &healthpb.HealthCheckRequest{})
	}
	_, err := c.Check(t.Context(), &healthpb.HealthCheckRequest{})
	if !errors.Is(err, grpcx.ErrCircuitOpen) || !errors.Is(err, errs.ErrDependencyUnavailable) {
		t.Fatalf("err %v, want an open breaker", err)
	}
	if f.calls.Load() != 3 {
		t.Errorf("calls %d, want 3 (the open breaker sends nothing)", f.calls.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(changes, ",") != "closed>open" {
		t.Errorf("state changes %v", changes)
	}
}

func TestConnState(t *testing.T) {
	c, err := grpcx.New("passthrough:///unused", grpcx.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	if s := c.State().String(); s != "IDLE" {
		t.Errorf("state %s, want IDLE: New must not connect", s)
	}
	if err := c.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
