package ratelimit_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/ratelimit"
)

// keyStore records bucket keys and allows everything.
type keyStore struct {
	mu   sync.Mutex
	keys []string
}

func (s *keyStore) Allow(_ context.Context, key string, r ratelimit.Rule) (ratelimit.Decision, error) {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
	return ratelimit.Decision{Allowed: true, Limit: r.Rate, Remaining: 1}, nil
}

// Bucket keys never carry a Redis Cluster hash tag (a route such as
// "GET /files/{id}" would put every bucket of the route in one slot) and a
// policy name cannot collide with another policy's keys.
func TestBucketKeysEscaped(t *testing.T) {
	rule := ratelimit.Rule{Rate: 5, Per: time.Second}
	for _, tt := range []struct {
		name         string
		policy, key  string
		ctx          context.Context
		want, reject string
	}{
		{name: "route hash tag", policy: "GET /files/{id}", ctx: ctxWith("u1", "", ""), want: "rl:GET /files/%7Bid%7D:user:u1"},
		{name: "user id hash tag", policy: "p", ctx: ctxWith("{slot}", "", ""), want: "rl:p:user:%7Bslot%7D"},
		{name: "custom key hash tag", policy: "p", key: "{t}", want: "rl:p:key:%7Bt%7D"},
		{name: "scope separator", policy: "x:key", ctx: ctxWith("5", "", ""), want: "rl:x%3Akey:user:5"},
		{name: "ipv6 untouched", policy: "p", ctx: ctxWith("", "", "2001:db8::1"), want: "rl:p:ip:2001:db8::/64"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := &keyStore{}
			l := mustNew(t, ratelimit.Config{Store: s, Policies: map[string]ratelimit.Policy{tt.policy: {Rule: rule}}})
			if tt.key != "" {
				_, _ = l.Allow(context.Background(), tt.policy, tt.key)
			} else {
				_, _ = l.Check(tt.ctx, tt.policy)
			}
			if len(s.keys) != 1 || s.keys[0] != tt.want {
				t.Fatalf("keys = %q, want [%q]", s.keys, tt.want)
			}
		})
	}
}

// IPv6Prefix groups the /64s of one delegated prefix into one IP bucket.
func TestIPv6Prefix(t *testing.T) {
	rule := ratelimit.Rule{Rate: 1, Per: time.Hour}
	a, b := ctxWith("", "", "2001:db8:0:1::1"), ctxWith("", "", "2001:db8:0:2::1") // same /56, other /64
	for _, tt := range []struct {
		bits       int
		wantSecond bool
	}{{0, true}, {64, true}, {56, false}, {48, false}} {
		l := mustNew(t, ratelimit.Config{IPv6Prefix: tt.bits, Default: &ratelimit.Policy{Rule: rule}})
		if d, _ := l.Check(a, "m"); !d.Allowed {
			t.Fatalf("/%d: first denied", tt.bits)
		}
		if d, _ := l.Check(b, "m"); d.Allowed != tt.wantSecond {
			t.Fatalf("/%d: second allowed = %v", tt.bits, d.Allowed)
		}
	}
	for _, bad := range []int{-1, 16, 129} {
		if _, err := ratelimit.New(ratelimit.Config{IPv6Prefix: bad, Default: &ratelimit.Policy{Rule: rule}}); err == nil {
			t.Fatalf("IPv6Prefix %d: want error", bad)
		}
	}
	l := mustNew(t, ratelimit.Config{IPv6Prefix: 56, Default: &ratelimit.Policy{Rule: rule}})
	if k, _ := l.Key(a, ""); k != "ip:2001:db8::/56" {
		t.Fatalf("Key = %q", k)
	}
}

// A store outage is logged when the breaker opens and when a probe fails,
// not once per request.
func TestBreakerLogsOncePerOutage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var buf bytes.Buffer
		var mu sync.Mutex
		store := &flakyStore{}
		store.fail.Store(true)
		l := mustNew(t, ratelimit.Config{
			Store: store, BreakerCooldown: time.Second,
			Logger:  log.New(&lockedWriter{w: &buf, mu: &mu}, log.Options{}),
			Default: &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 1000, Per: time.Second}},
		})
		h := l.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		check := func() {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = "1.2.3.4:5"
			h.ServeHTTP(httptest.NewRecorder(), req)
		}
		for range 50 {
			check()
		}
		mu.Lock()
		if n := strings.Count(buf.String(), "store fail"); n != 1 {
			t.Fatalf("after 50 failing requests: %d warnings, want 1\n%s", n, buf.String())
		}
		mu.Unlock()
		time.Sleep(1500 * time.Millisecond)
		for range 10 {
			check() // one probe fails
		}
		store.fail.Store(false)
		time.Sleep(1500 * time.Millisecond)
		for range 10 {
			check() // the probe succeeds
		}
		mu.Lock()
		defer mu.Unlock()
		if n := strings.Count(buf.String(), "store fail"); n != 2 {
			t.Fatalf("%d warnings, want 2\n%s", n, buf.String())
		}
		if n := strings.Count(buf.String(), "store recovered"); n != 1 {
			t.Fatalf("%d recoveries, want 1\n%s", n, buf.String())
		}
	})
}

type lockedWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

// OnStoreError: Deny answers 503 with Retry-After over HTTP, as gRPC gets
// retry-after metadata.
func TestUnavailableHTTPRetryAfter(t *testing.T) {
	l := mustNew(t, ratelimit.Config{
		Store: failingStore{}, BreakerCooldown: 2 * time.Second,
		Policies: map[string]ratelimit.Policy{"GET /x": {Rule: ratelimit.Rule{Rate: 1, Per: time.Second}, OnStoreError: ratelimit.Deny}},
		Logger:   log.Discard(),
	})
	mux := http.NewServeMux()
	mux.Handle("GET /x", l.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.RemoteAddr = "1.2.3.4:5"
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "2" {
		t.Fatalf("%d Retry-After=%q %s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
}

// RateLimit-Policy has whole-second windows only: a sub-second Per is
// scaled, not reported as Rate per second.
func TestPolicyHeaderSubSecond(t *testing.T) {
	for _, tt := range []struct {
		rule ratelimit.Rule
		want string
	}{
		{ratelimit.Rule{Rate: 1, Per: 100 * time.Millisecond}, `"GET /x";q=10;w=1`},
		{ratelimit.Rule{Rate: 3, Per: 1500 * time.Millisecond}, `"GET /x";q=4;w=2`},
		{ratelimit.Rule{Rate: 7, Per: time.Minute}, `"GET /x";q=7;w=60`},
	} {
		l := mustNew(t, ratelimit.Config{Policies: map[string]ratelimit.Policy{"GET /x": {Rule: tt.rule}}})
		mux := http.NewServeMux()
		mux.Handle("GET /x", l.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})))
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		req.RemoteAddr = "1.2.3.4:5"
		mux.ServeHTTP(rec, req)
		if got := rec.Header().Get("RateLimit-Policy"); got != tt.want {
			t.Errorf("%+v: RateLimit-Policy = %s, want %s", tt.rule, got, tt.want)
		}
	}
}
