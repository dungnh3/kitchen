package ratelimit_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/ratelimit"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/goleak"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestLocalStoreGCRA(t *testing.T) {
	s := ratelimit.NewLocalStore(100)
	r := ratelimit.Rule{Rate: 2, Per: time.Second, Burst: 3}
	ctx := context.Background()
	for i := range 3 {
		d, err := s.Allow(ctx, "k", r)
		if err != nil || !d.Allowed || d.Remaining != 2-i || d.Limit != 2 {
			t.Fatalf("request %d: %+v %v", i, d, err)
		}
	}
	d, _ := s.Allow(ctx, "k", r)
	if d.Allowed || d.RetryAfter <= 0 || d.RetryAfter > 500*time.Millisecond {
		t.Fatalf("4th request: %+v", d)
	}
	if d, _ := s.Allow(ctx, "other", r); !d.Allowed {
		t.Fatal("keys must be independent")
	}
}

func TestLocalStoreLRUBound(t *testing.T) {
	s := ratelimit.NewLocalStore(64)
	r := ratelimit.Rule{Rate: 1, Per: time.Minute}
	for i := range 1000 {
		_, _ = s.Allow(context.Background(), string(rune(i)), r)
	}
	if n := s.Len(); n > 64 {
		t.Fatalf("Len = %d, want <= 64", n)
	}
}

func TestNewValidates(t *testing.T) {
	bad := []ratelimit.Config{
		{Default: &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 0, Per: time.Second}}},
		{Policies: map[string]ratelimit.Policy{"/a/B": {Rule: ratelimit.Rule{Rate: 1}}}},
		{Policies: map[string]ratelimit.Policy{"*": {Rule: ratelimit.Rule{Rate: 1, Per: time.Second}}}},
	}
	for i, c := range bad {
		if _, err := ratelimit.New(c); err == nil {
			t.Errorf("config %d: want error", i)
		}
	}
}

func TestPoliciesAndDefault(t *testing.T) {
	l, err := ratelimit.New(ratelimit.Config{
		Default:  &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}},
		Policies: map[string]ratelimit.Policy{"/s/Hot": {Rule: ratelimit.Rule{Rate: 2, Per: time.Hour}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	allowed := func(name string) bool { d, _ := l.Allow(ctx, name, "ip:1.2.3.4"); return d.Allowed }
	if !allowed("/s/A") || allowed("/s/B") {
		t.Fatal("default bucket is shared across methods without a policy")
	}
	if !allowed("/s/Hot") || !allowed("/s/Hot") || allowed("/s/Hot") {
		t.Fatal("policy has its own bucket")
	}

	none, _ := ratelimit.New(ratelimit.Config{})
	if d, _ := none.Allow(ctx, "/s/A", "k"); !d.Allowed || none.Limited("/s/A") {
		t.Fatal("no policy means unlimited")
	}
}

func TestDryRun(t *testing.T) {
	l, _ := ratelimit.New(ratelimit.Config{Default: &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}, DryRun: true}})
	for range 3 {
		if d, _ := l.Allow(context.Background(), "m", "k"); !d.Allowed {
			t.Fatal("dry run must not block")
		}
	}
}

type flakyStore struct {
	fail  atomic.Bool
	calls atomic.Int64
}

func (f *flakyStore) Allow(ctx context.Context, _ string, r ratelimit.Rule) (ratelimit.Decision, error) {
	f.calls.Add(1)
	if f.fail.Load() {
		return ratelimit.Decision{}, errors.New("connection refused")
	}
	return ratelimit.Decision{Allowed: true, Limit: r.Rate, Remaining: r.Rate - 1}, nil
}

func TestStoreErrorModes(t *testing.T) {
	ctx := context.Background()
	rule := ratelimit.Rule{Rate: 4, Per: time.Hour}
	for _, tt := range []struct {
		mode        ratelimit.StoreErrorMode
		wantAllowed []bool
	}{
		{ratelimit.FallbackLocal, []bool{true, true, false}}, // 4 / 2 instances = 2
		{ratelimit.Allow, []bool{true, true, true}},
		{ratelimit.Deny, []bool{false, false, false}},
	} {
		store := &flakyStore{}
		store.fail.Store(true)
		l, _ := ratelimit.New(ratelimit.Config{
			Store: store, Instances: 2, BreakerCooldown: time.Hour,
			Default: &ratelimit.Policy{Rule: rule, OnStoreError: tt.mode},
		})
		for i, want := range tt.wantAllowed {
			d, err := l.Allow(ctx, "m", "k")
			if d.Allowed != want || !d.Degraded || err == nil {
				t.Errorf("mode %d request %d: %+v %v", tt.mode, i, d, err)
			}
			if tt.mode == ratelimit.Deny && !d.Unavailable {
				t.Errorf("Deny should mark Unavailable")
			}
		}
		if got := store.calls.Load(); got != 1 {
			t.Errorf("mode %d: breaker should stop calls after the first failure, got %d", tt.mode, got)
		}
	}
}

func TestBreakerSingleProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &flakyStore{}
		store.fail.Store(true)
		l, _ := ratelimit.New(ratelimit.Config{
			Store: store, BreakerCooldown: time.Millisecond,
			Default: &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 1000, Per: time.Second}},
		})
		ctx := context.Background()
		_, _ = l.Allow(ctx, "m", "k")    // opens
		time.Sleep(5 * time.Millisecond) // fake clock inside the bubble
		store.fail.Store(false)
		var wg sync.WaitGroup
		for range 50 {
			wg.Go(func() { _, _ = l.Allow(ctx, "m", "k") })
		}
		wg.Wait()
		// After the successful probe the breaker closes and calls flow again.
		before := store.calls.Load()
		if d, err := l.Allow(ctx, "m", "k"); err != nil || d.Degraded {
			t.Fatalf("breaker should be closed: %+v %v", d, err)
		}
		if store.calls.Load() != before+1 {
			t.Fatal("closed breaker should call the store")
		}
	})
}

func TestBreakerIgnoresCallerCancel(t *testing.T) {
	store := &blockingStore{}
	l, _ := ratelimit.New(ratelimit.Config{
		Store: store, BreakerCooldown: time.Hour,
		Default: &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 10, Per: time.Second}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = l.Allow(ctx, "m", "k")
	store.ok.Store(true)
	if d, err := l.Allow(context.Background(), "m", "k"); err != nil || d.Degraded {
		t.Fatalf("a canceled caller must not open the breaker: %+v %v", d, err)
	}
}

type blockingStore struct{ ok atomic.Bool }

func (b *blockingStore) Allow(ctx context.Context, _ string, r ratelimit.Rule) (ratelimit.Decision, error) {
	if b.ok.Load() {
		return ratelimit.Decision{Allowed: true, Limit: r.Rate}, nil
	}
	<-ctx.Done()
	return ratelimit.Decision{}, ctx.Err()
}

func TestStoreTimeout(t *testing.T) {
	l, _ := ratelimit.New(ratelimit.Config{
		Store: &blockingStore{}, StoreTimeout: 10 * time.Millisecond,
		Default: &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 10, Per: time.Second}},
	})
	start := time.Now()
	d, err := l.Allow(context.Background(), "m", "k")
	if err == nil || !d.Degraded || !d.Allowed || time.Since(start) > time.Second {
		t.Fatalf("timeout not applied: %+v %v %s", d, err, time.Since(start))
	}
}

func TestKey(t *testing.T) {
	trustedNet := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	l, _ := ratelimit.New(ratelimit.Config{
		TrustedProxies: trustedNet,
		Subject: func(ctx context.Context) (string, bool) {
			s, ok := ctx.Value(subKey{}).(string)
			return s, ok
		},
	})
	ctx := context.Background()
	tests := []struct {
		name   string
		ctx    context.Context
		remote string
		xff    []string
		want   string
		ok     bool
	}{
		{"subject wins", context.WithValue(ctx, subKey{}, "u1"), "1.1.1.1:5", nil, "sub:u1", true},
		{"direct client", ctx, "1.1.1.1:5", []string{"9.9.9.9"}, "ip:1.1.1.1", true}, // untrusted peer: XFF ignored
		{"via trusted proxy", ctx, "10.0.0.2:5", []string{"9.9.9.9, 10.0.0.3"}, "ip:9.9.9.9", true},
		{"spoofed left entries ignored", ctx, "10.0.0.2:5", []string{"6.6.6.6, 9.9.9.9"}, "ip:9.9.9.9", true},
		{"ipv6 /64", ctx, "[2001:db8:1:2:3:4:5:6]:443", nil, "ip:2001:db8:1:2::/64", true},
		{"mapped v4", ctx, "[::ffff:1.2.3.4]:1", nil, "ip:1.2.3.4", true},
		{"unknown", ctx, "bufconn", nil, "", false},
	}
	for _, tt := range tests {
		got, ok := l.Key(tt.ctx, tt.remote, tt.xff)
		if got != tt.want || ok != tt.ok {
			t.Errorf("%s: %q %v, want %q %v", tt.name, got, ok, tt.want, tt.ok)
		}
	}
}

type subKey struct{}

func TestUnaryInterceptor(t *testing.T) {
	l, _ := ratelimit.New(ratelimit.Config{
		Policies: map[string]ratelimit.Policy{"/s/M": {Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}}},
	})
	ic := l.UnaryServerInterceptor()
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 9}})
	ctx = metadata.NewIncomingContext(ctx, metadata.MD{})
	h := func(context.Context, any) (any, error) { return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: "/s/M"}
	if _, err := ic(ctx, nil, info, h); err != nil {
		t.Fatal(err)
	}
	_, err := ic(ctx, nil, info, h)
	st := status.Convert(err)
	if st.Code() != codes.ResourceExhausted || !errors.Is(err, ratelimit.ErrRateLimited) {
		t.Fatalf("got %v", err)
	}
	var ri *errdetails.RetryInfo
	for _, d := range st.Details() {
		if v, ok := d.(*errdetails.RetryInfo); ok {
			ri = v
		}
	}
	if ri == nil || ri.GetRetryDelay().AsDuration() <= 0 {
		t.Fatalf("RetryInfo missing: %v", st.Details())
	}
	if _, err := ic(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/s/Other"}, h); err != nil {
		t.Fatal("methods without policy are not limited")
	}
}

func TestHTTPMiddleware(t *testing.T) {
	l, _ := ratelimit.New(ratelimit.Config{
		Policies: map[string]ratelimit.Policy{"POST /v1/chat": {Rule: ratelimit.Rule{Rate: 1, Per: time.Minute}}},
	})
	mux := http.NewServeMux()
	mux.Handle("POST /v1/chat", l.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})))
	do := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat", nil)
		req.RemoteAddr = "1.2.3.4:5"
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}
	if rec := do(); rec.Code != http.StatusNoContent || rec.Header().Get("X-RateLimit-Limit") != "1" || rec.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Fatalf("first: %d %v", rec.Code, rec.Header())
	}
	rec := do()
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("second: %d %v %s", rec.Code, rec.Header(), rec.Body.String())
	}
	if errs.HTTPStatusFromCode(codes.ResourceExhausted) != rec.Code {
		t.Fatal("mapping mismatch")
	}
}

func TestMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()
	l, _ := ratelimit.New(ratelimit.Config{
		MeterProvider: mp,
		Default:       &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}},
	})
	_, _ = l.Allow(context.Background(), "m", "k")
	_, _ = l.Allow(context.Background(), "m", "k")
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	results := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "kitchen.ratelimit.decisions" {
				continue
			}
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				r, _ := dp.Attributes.Value("result")
				results[r.AsString()] += dp.Value
			}
		}
	}
	if results["allowed"] != 1 || results["limited"] != 1 {
		t.Fatalf("results = %v", results)
	}
}
