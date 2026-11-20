package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dungnh3/kitchen/ratelimit"
	rlredis "github.com/dungnh3/kitchen/ratelimit/redis"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func newStore(t *testing.T) (*rlredis.Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = rdb.Close() })
	s, err := rlredis.New(rdb)
	if err != nil {
		t.Fatal(err)
	}
	return s, mr
}

func TestNewRequiresContextTimeout(t *testing.T) {
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer func() { _ = rdb.Close() }()
	if _, err := rlredis.New(rdb); err == nil {
		t.Fatal("want error without ContextTimeoutEnabled")
	}
	if _, err := rlredis.New(nil); err == nil {
		t.Fatal("want error for nil client")
	}
}

func TestGCRA(t *testing.T) {
	s, mr := newStore(t)
	ctx := context.Background()
	r := ratelimit.Rule{Rate: 2, Per: time.Second, Burst: 3}
	for i := range 3 {
		d, err := s.Allow(ctx, "rl:*:ip:1.2.3.4", r)
		if err != nil || !d.Allowed || d.Remaining != 2-i || d.Limit != 2 {
			t.Fatalf("request %d: %+v %v", i, d, err)
		}
	}
	d, err := s.Allow(ctx, "rl:*:ip:1.2.3.4", r)
	if err != nil || d.Allowed {
		t.Fatalf("4th: %+v %v", d, err)
	}
	if d.RetryAfter < 400*time.Millisecond || d.RetryAfter > 600*time.Millisecond {
		t.Fatalf("RetryAfter = %s, want ~500ms", d.RetryAfter)
	}
	if d.ResetAfter < 1400*time.Millisecond || d.ResetAfter > 1600*time.Millisecond {
		t.Fatalf("ResetAfter = %s, want ~1.5s", d.ResetAfter)
	}
	if ttl := mr.TTL("rl:*:ip:1.2.3.4"); ttl <= 0 || ttl > 2*time.Second {
		t.Fatalf("key TTL = %s", ttl)
	}

	// Server time moves on: one request per 500ms comes back.
	mr.SetTime(time.Now().Add(600 * time.Millisecond))
	if d, _ := s.Allow(ctx, "rl:*:ip:1.2.3.4", r); !d.Allowed {
		t.Fatalf("after refill: %+v", d)
	}
}

func TestStoreError(t *testing.T) {
	s, mr := newStore(t)
	mr.Close()
	if _, err := s.Allow(context.Background(), "k", ratelimit.Rule{Rate: 1, Per: time.Second}); err == nil {
		t.Fatal("want error when Redis is down")
	}
}

func TestWithLimiterFallback(t *testing.T) {
	s, mr := newStore(t)
	l, err := ratelimit.New(ratelimit.Config{
		Store:   s,
		Default: &ratelimit.Policy{Rule: ratelimit.Rule{Rate: 1, Per: time.Hour}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if d, err := l.Allow(ctx, "m", "k"); err != nil || !d.Allowed || d.Degraded {
		t.Fatalf("redis path: %+v %v", d, err)
	}
	mr.Close()
	d, err := l.Allow(ctx, "m", "k")
	if err == nil || !d.Degraded || !d.Allowed {
		t.Fatalf("fallback path: %+v %v", d, err)
	}
}
