package ratelimit_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/ratelimit"
	"github.com/dungnh3/kitchen/server"
)

func Example_server() {
	// Users, anonymous subjects and devices 20/s each, unauthenticated
	// client IPs 300/s, for every method without its own policy.
	limiter, err := ratelimit.New(ratelimit.Config{
		Default: &ratelimit.Policy{Limits: ratelimit.DefaultLimits()},
	})
	if err != nil {
		panic(err)
	}
	// The client IP is only trusted from these proxies (the load balancer).
	proxies, err := xcontext.NewTrustedProxies("10.0.0.0/8")
	if err != nil {
		panic(err)
	}
	srv, err := server.New(
		// server.WithAuth(authn) first: signed-in callers are then limited
		// per user, everyone else per client IP.
		server.WithRateLimit(limiter),
		server.WithTrustedProxies(proxies),
		server.WithServices( /* your services */ ),
	)
	if err != nil {
		panic(err)
	}
	_ = srv // srv.Run(ctx) in main
}

func ExampleLimiter_Middleware() {
	l, err := ratelimit.New(ratelimit.Config{
		Policies: map[string]ratelimit.Policy{
			"GET /hello": {Rule: ratelimit.Rule{Rate: 1, Per: time.Minute}},
		},
	})
	if err != nil {
		panic(err)
	}
	// Without package server: wrap handlers registered on a ServeMux, so the
	// policy is looked up by the matched pattern.
	mux := http.NewServeMux()
	mux.Handle("GET /hello", l.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "hi")
	})))
	for range 2 {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/hello", nil))
		fmt.Println(rec.Code, rec.Header().Get("Retry-After"), rec.Header().Get("RateLimit"))
	}
	// Output:
	// 200  "GET /hello";r=0;t=60
	// 429 60 "GET /hello";r=0;t=60
}

func ExampleLimiter_LimitHTTP() {
	l, err := ratelimit.New(ratelimit.Config{
		Policies: map[string]ratelimit.Policy{
			"POST /v1/upload": {Rule: ratelimit.Rule{Rate: 1, Per: time.Second}},
		},
	})
	if err != nil {
		panic(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/upload", nil)
	_ = l.LimitHTTP(httptest.NewRecorder(), r, "POST /v1/upload")
	err = l.LimitHTTP(httptest.NewRecorder(), r, "POST /v1/upload")
	// A denial is an errs error: render it with errs.Handler.WriteHTTP.
	fmt.Println(errors.Is(err, ratelimit.ErrRateLimited), errs.Code(err), errs.HTTPStatus(err))
	// Output:
	// true ResourceExhausted 429
}

func ExampleLimiter_Allow() {
	l, err := ratelimit.New(ratelimit.Config{
		Policies: map[string]ratelimit.Policy{
			"export": {Rule: ratelimit.Rule{Rate: 2, Per: time.Hour}},
		},
	})
	if err != nil {
		panic(err)
	}
	// A bucket chosen by the caller, here one per tenant. Keys live in their
	// own namespace, so they never drain a user or IP bucket.
	for range 3 {
		d, _ := l.Allow(context.Background(), "export", "tenant:42")
		fmt.Println(d.Allowed)
	}
	// Output:
	// true
	// true
	// false
}

type tenantKey struct{}

func ExampleLimit_key() {
	l, err := ratelimit.New(ratelimit.Config{
		Policies: map[string]ratelimit.Policy{
			"/search.v1.Search/Query": {Limits: []ratelimit.Limit{
				// Every tenant shares 1 000/min across its users...
				{Rule: ratelimit.Rule{Rate: 1000, Per: time.Minute}, Key: func(ctx context.Context) (string, bool) {
					t, ok := ctx.Value(tenantKey{}).(string)
					return t, ok
				}},
				// ...and each caller still gets at most 10/s.
				{Rule: ratelimit.Rule{Rate: 10, Per: time.Second}, By: ratelimit.ByCaller},
			}},
		},
	})
	if err != nil {
		panic(err)
	}
	ctx := xcontext.WithUserID(context.WithValue(context.Background(), tenantKey{}, "acme"), "u1")
	d, _ := l.Check(ctx, "/search.v1.Search/Query")
	fmt.Println(d.Allowed)
	// Output:
	// true
}

func ExampleConfig_dryRun() {
	l, err := ratelimit.New(ratelimit.Config{
		Policies: map[string]ratelimit.Policy{
			"send-otp": {Rule: ratelimit.Rule{Rate: 1, Per: time.Minute}},
		},
		// Count and report, never block: roll new limits out like this and
		// read the "dry_run" results before enforcing.
		DryRun: true,
		OnDecision: func(_ context.Context, e ratelimit.Event) {
			fmt.Println(e.Policy, e.Limit, e.Result)
		},
	})
	if err != nil {
		panic(err)
	}
	ctx := xcontext.WithUserID(context.Background(), "u1")
	for range 2 {
		d, _ := l.Check(ctx, "send-otp")
		fmt.Println(d.Allowed)
	}
	// Output:
	// send-otp caller allowed
	// true
	// send-otp caller dry_run
	// true
}

// downStore is a Store whose backend is unreachable.
type downStore struct{}

func (downStore) Allow(context.Context, string, ratelimit.Rule) (ratelimit.Decision, error) {
	return ratelimit.Decision{}, errors.New("connection refused")
}

func ExamplePolicy_onStoreError() {
	l, err := ratelimit.New(ratelimit.Config{
		Store: downStore{},
		Policies: map[string]ratelimit.Policy{
			// Login brute force must stay blocked while Redis is down.
			"/auth.v1.Auth/Login": {Rule: ratelimit.Rule{Rate: 5, Per: time.Minute}, OnStoreError: ratelimit.Deny},
			// Search degrades to a per-instance limit (the default).
			"/search.v1.Search/Query": {Rule: ratelimit.Rule{Rate: 10, Per: time.Second}},
		},
	})
	if err != nil {
		panic(err)
	}
	ctx := xcontext.WithUserID(context.Background(), "u1")
	d, _ := l.Check(ctx, "/auth.v1.Auth/Login")
	fmt.Println(d.Allowed, d.Unavailable)
	d, _ = l.Check(ctx, "/search.v1.Search/Query")
	fmt.Println(d.Allowed, d.Degraded)
	// Output:
	// false true
	// true true
}
