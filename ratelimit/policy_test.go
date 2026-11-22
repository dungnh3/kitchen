package ratelimit_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/ratelimit"
)

func ctxWith(user, device, ip string) context.Context {
	ctx := context.Background()
	info := xcontext.Info{DeviceID: device}
	if ip != "" {
		info.ClientIP = netip.MustParseAddr(ip)
	}
	ctx = xcontext.NewContext(ctx, info)
	if user != "" {
		ctx = xcontext.WithUserID(ctx, user)
	}
	return ctx
}

func mustNew(t *testing.T, cfg ratelimit.Config) *ratelimit.Limiter {
	t.Helper()
	l, err := ratelimit.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func allowedN(ctx context.Context, t *testing.T, l *ratelimit.Limiter, name string, n int) int {
	t.Helper()
	ok := 0
	for range n {
		d, err := l.Check(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if d.Allowed {
			ok++
		}
	}
	return ok
}

// The identity ladder: users, devices and IPs count separately; a device is
// also held by its IP bucket, so rotating fake ids does not help; users
// behind a busy IP are not affected.
func TestDefaultLimitsLadder(t *testing.T) {
	per := ratelimit.Rule{Rate: 2, Per: time.Hour}
	l := mustNew(t, ratelimit.Config{Default: &ratelimit.Policy{Limits: []ratelimit.Limit{
		{Rule: per, By: ratelimit.ByUser},
		{Rule: per, By: ratelimit.ByDevice},
		{Rule: ratelimit.Rule{Rate: 3, Per: time.Hour}, By: ratelimit.ByIP},
	}}})
	if got := allowedN(ctxWith("u1", "", "1.1.1.1"), t, l, "m", 5); got != 2 {
		t.Fatalf("user: %d allowed, want 2", got)
	}
	// Device d1 gets 2; rotating to d2 and d3 is stopped by the IP bucket (3).
	got := allowedN(ctxWith("", "d1", "2.2.2.2"), t, l, "m", 5) +
		allowedN(ctxWith("", "d2", "2.2.2.2"), t, l, "m", 5) +
		allowedN(ctxWith("", "d3", "2.2.2.2"), t, l, "m", 5)
	if got != 3 {
		t.Fatalf("device rotation: %d allowed, want 3 (IP bucket)", got)
	}
	// A user on the exhausted IP is still served (users skip ByIP).
	if got := allowedN(ctxWith("u2", "", "2.2.2.2"), t, l, "m", 1); got != 1 {
		t.Fatal("user behind a busy IP must not be limited by ByIP")
	}
}

func TestAnyIPAndGlobal(t *testing.T) {
	l := mustNew(t, ratelimit.Config{Policies: map[string]ratelimit.Policy{
		"ip":     {Limits: []ratelimit.Limit{{Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}, By: ratelimit.ByAnyIP}}},
		"global": {Limits: []ratelimit.Limit{{Rule: ratelimit.Rule{Rate: 2, Per: time.Hour}, By: ratelimit.ByGlobal}}},
	}})
	if allowedN(ctxWith("u1", "", "3.3.3.3"), t, l, "ip", 1)+allowedN(ctxWith("u2", "", "3.3.3.3"), t, l, "ip", 1) != 1 {
		t.Fatal("ByAnyIP must count authenticated users by IP too")
	}
	if allowedN(ctxWith("a", "", "4.4.4.4"), t, l, "global", 2)+allowedN(ctxWith("b", "", "5.5.5.5"), t, l, "global", 2) != 2 {
		t.Fatal("ByGlobal must share one bucket")
	}
}

func TestAnonymousAndCustomKey(t *testing.T) {
	type anonKey struct{}
	type tenantKey struct{}
	l := mustNew(t, ratelimit.Config{
		Anonymous: func(ctx context.Context) (string, bool) { s, ok := ctx.Value(anonKey{}).(string); return s, ok },
		Policies: map[string]ratelimit.Policy{"m": {Limits: []ratelimit.Limit{
			{Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}, By: ratelimit.ByAnon},
			{Rule: ratelimit.Rule{Rate: 2, Per: time.Hour}, Key: func(ctx context.Context) (string, bool) {
				s, ok := ctx.Value(tenantKey{}).(string)
				return s, ok
			}},
		}}},
	})
	anon := context.WithValue(ctxWith("anon-sub", "", "6.6.6.6"), anonKey{}, "a1")
	if got := allowedN(anon, t, l, "m", 3); got != 1 {
		t.Fatalf("anon: %d allowed, want 1", got)
	}
	tenant := context.WithValue(ctxWith("u", "", "7.7.7.7"), tenantKey{}, "t1")
	if got := allowedN(tenant, t, l, "m", 3); got != 2 {
		t.Fatalf("tenant: %d allowed, want 2", got)
	}
}

func TestExemptOverridesSkipDisabled(t *testing.T) {
	one := ratelimit.Rule{Rate: 1, Per: time.Hour}
	l := mustNew(t, ratelimit.Config{
		Default:   &ratelimit.Policy{Rule: one},
		Exempt:    []string{"ip:10.0.0.0/8", "user:admin", "ip:2001:db8:1::1"},
		Overrides: map[string]ratelimit.Rule{"user:partner": {Rate: 5, Per: time.Hour}, "ip:2001:db8::1": {Rate: 3, Per: time.Hour}},
		Skip:      []string{"/grpc.Free/Call"},
	})
	for name, ctx := range map[string]context.Context{
		"internal CIDR": ctxWith("", "", "10.1.2.3"),
		"exempt user":   ctxWith("admin", "", "8.8.8.8"),
		"exempt ipv6":   ctxWith("", "", "2001:db8:1::1"),
	} {
		if got := allowedN(ctx, t, l, "m", 5); got != 5 {
			t.Errorf("%s: %d allowed, want 5", name, got)
		}
	}
	if got := allowedN(ctxWith("partner", "", "1.2.3.4"), t, l, "m", 9); got != 5 {
		t.Errorf("override user: %d, want 5", got)
	}
	// Overrides on an IPv6 address apply to its /64.
	if got := allowedN(ctxWith("", "", "2001:db8::2"), t, l, "m", 9); got != 3 {
		t.Errorf("override ipv6 /64: %d, want 3", got)
	}
	if got := allowedN(ctxWith("x", "", "1.1.1.1"), t, l, "/grpc.Free/Call", 5); got != 5 || l.Limited("/grpc.Free/Call") {
		t.Errorf("skip: %d", got)
	}
	// An IPv6 exemption is that address only, not its /64.
	if got := allowedN(ctxWith("", "", "2001:db8:1::dead"), t, l, "m", 3); got != 1 {
		t.Errorf("ipv6 neighbour exempt: %d allowed, want 1", got)
	}
	off := mustNew(t, ratelimit.Config{Default: &ratelimit.Policy{Rule: one}, Disabled: true})
	if off.Limited("m") || allowedN(ctxWith("x", "", "1.1.1.1"), t, off, "m", 3) != 3 {
		t.Error("disabled limiter must allow everything")
	}
}

func TestDryRunAndOnDecision(t *testing.T) {
	var mu sync.Mutex
	var events []ratelimit.Event
	l := mustNew(t, ratelimit.Config{
		Default: &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}},
		DryRun:  true,
		OnDecision: func(_ context.Context, e ratelimit.Event) {
			mu.Lock()
			events = append(events, e)
			mu.Unlock()
		},
	})
	if got := allowedN(ctxWith("u", "", "1.1.1.1"), t, l, "m", 3); got != 3 {
		t.Fatalf("global dry run blocked: %d", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 3 || events[0].Result != "allowed" || events[2].Result != "dry_run" || events[2].Policy != "default" || events[2].Limit != "caller" {
		t.Fatalf("events = %+v", events)
	}
}

func TestMultiLimitHeaders(t *testing.T) {
	l := mustNew(t, ratelimit.Config{Policies: map[string]ratelimit.Policy{"POST /v1/chat": {Limits: []ratelimit.Limit{
		{Rule: ratelimit.Rule{Rate: 5, Per: time.Minute}, By: ratelimit.ByAnyIP},
		{Rule: ratelimit.Rule{Rate: 100, Per: 24 * time.Hour}, By: ratelimit.ByAnyIP},
	}}}})
	mux := http.NewServeMux()
	mux.Handle("POST /v1/chat", l.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })))
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/chat", nil)
	req.RemoteAddr = "1.2.3.4:5"
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	h := rec.Header()
	if rec.Code != http.StatusNoContent ||
		h.Get("RateLimit-Policy") != `"POST /v1/chat#any_ip";q=5;w=60, "POST /v1/chat#any_ip-2";q=100;w=86400` ||
		!strings.HasPrefix(h.Get("RateLimit"), `"POST /v1/chat#any_ip";r=4;t=`) ||
		h.Get("X-RateLimit-Limit") != "5" || h.Get("X-RateLimit-Remaining") != "4" {
		t.Fatalf("%d %v", rec.Code, h)
	}
}

func TestAllowCustomKeyAndConfigErrors(t *testing.T) {
	l := mustNew(t, ratelimit.Config{Policies: map[string]ratelimit.Policy{"m": {Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}}}})
	if d, _ := l.Allow(context.Background(), "m", "tenant:1"); !d.Allowed {
		t.Fatal("first")
	}
	if d, _ := l.Allow(context.Background(), "m", "tenant:1"); d.Allowed {
		t.Fatal("second must be denied")
	}
	for name, cfg := range map[string]ratelimit.Config{
		"empty policy":  {Policies: map[string]ratelimit.Policy{"m": {}}},
		"bad exempt":    {Exempt: []string{"ip:nope"}},
		"bad kind":      {Exempt: []string{"team:x"}},
		"override cidr": {Overrides: map[string]ratelimit.Rule{"ip:10.0.0.0/8": {Rate: 1, Per: time.Second}}},
		"override rule": {Overrides: map[string]ratelimit.Rule{"user:x": {}}},
		"unknown By":    {Policies: map[string]ratelimit.Policy{"m": {Limits: []ratelimit.Limit{{Rule: ratelimit.Rule{Rate: 1, Per: time.Second}, By: 42}}}}},
		"reserved name": {Policies: map[string]ratelimit.Policy{"*": {Rule: ratelimit.Rule{Rate: 1, Per: time.Second}}}},
	} {
		if _, err := ratelimit.New(cfg); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

// Review 2026-11-21: a ByCaller limit cannot be bypassed by rotating the
// unverified device id (the client IP is counted too); users are not
// affected by that IP bucket.
func TestByCallerDeviceRotation(t *testing.T) {
	l := mustNew(t, ratelimit.Config{Policies: map[string]ratelimit.Policy{
		"POST /otp": {Rule: ratelimit.Rule{Rate: 5, Per: time.Minute}},
	}})
	got := 0
	for i := range 200 {
		got += allowedN(ctxWith("", "dev-"+strconv.Itoa(i), "9.9.9.9"), t, l, "POST /otp", 1)
	}
	if got != 5 {
		t.Fatalf("device rotation: %d allowed, want 5", got)
	}
	if allowedN(ctxWith("u1", "", "9.9.9.9"), t, l, "POST /otp", 1) != 1 {
		t.Fatal("a signed-in user on that IP must not be limited by it")
	}
}

// DryRun never blocks, not even when the store is down and the policy says
// Deny.
func TestDryRunDuringOutage(t *testing.T) {
	store := &failingStore{}
	l := mustNew(t, ratelimit.Config{
		Store: store, DryRun: true,
		Default: &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}, OnStoreError: ratelimit.Deny},
	})
	d, _ := l.Check(ctxWith("u", "", "1.1.1.1"), "m")
	if !d.Allowed || d.Unavailable {
		t.Fatalf("dry run blocked during outage: %+v", d)
	}
}

type failingStore struct{}

func (failingStore) Allow(context.Context, string, ratelimit.Rule) (ratelimit.Decision, error) {
	return ratelimit.Decision{}, errors.New("redis down")
}

// Allow keys live in their own namespace.
func TestAllowKeyNamespace(t *testing.T) {
	l := mustNew(t, ratelimit.Config{Default: &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}}})
	if d, _ := l.Allow(context.Background(), "m", "user:victim"); !d.Allowed {
		t.Fatal("first custom key")
	}
	if got := allowedN(ctxWith("victim", "", "1.1.1.1"), t, l, "m", 1); got != 1 {
		t.Fatal("Allow(\"user:victim\") drained the victim's bucket")
	}
}
