package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
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
	call := func() (bool, int, time.Duration, time.Duration, error) {
		return s.GCRA(ctx, "rl:*:ip:1.2.3.4", 2, 3, time.Second)
	}
	for i := range 3 {
		ok, rem, _, _, err := call()
		if err != nil || !ok || rem != 2-i {
			t.Fatalf("request %d: %v %d %v", i, ok, rem, err)
		}
	}
	ok, _, retry, reset, err := call()
	if err != nil || ok {
		t.Fatalf("4th: %v %v", ok, err)
	}
	if retry < 400*time.Millisecond || retry > 600*time.Millisecond {
		t.Fatalf("retryAfter = %s, want ~500ms", retry)
	}
	if reset < 1400*time.Millisecond || reset > 1600*time.Millisecond {
		t.Fatalf("resetAfter = %s, want ~1.5s", reset)
	}
	if ttl := mr.TTL("rl:*:ip:1.2.3.4"); ttl <= 0 || ttl > 2*time.Second {
		t.Fatalf("key TTL = %s", ttl)
	}

	// Server time moves on: one request per 500ms comes back.
	mr.SetTime(time.Now().Add(600 * time.Millisecond))
	if ok, _, _, _, _ := call(); !ok {
		t.Fatal("after refill: denied")
	}
}

func TestStoreError(t *testing.T) {
	s, mr := newStore(t)
	mr.Close()
	if _, _, _, _, err := s.GCRA(context.Background(), "k", 1, 1, time.Second); err == nil {
		t.Fatal("want error when Redis is down")
	}
}

func TestInvalidRule(t *testing.T) {
	s, _ := newStore(t)
	if _, _, _, _, err := s.GCRA(context.Background(), "k", 0, 0, time.Second); err == nil {
		t.Fatal("want error for rate 0")
	}
}
