package redis_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dungnh3/kitchen/ratelimit"
	rlredis "github.com/dungnh3/kitchen/ratelimit/redis"
	goredis "github.com/redis/go-redis/v9"
)

// TestDifferentialLocalVsRedis drives the in-memory store and the Redis Lua
// script with the same random requests on the same clock and requires the
// same Decision every time (CLAUDE.md: both stores must decide alike).
// synctest gives LocalStore a fake clock; miniredis gets the same time.
func TestDifferentialLocalVsRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr(), ContextTimeoutEnabled: true, PoolSize: 1})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	s, err := rlredis.New(rdb)
	if err != nil {
		t.Fatal(err)
	}
	remote := ratelimit.RemoteStore(s)
	base := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC)

	for _, seed := range []uint64{1, 2, 3} {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				local := ratelimit.NewLocalStore(1000)
				rng := rand.New(rand.NewPCG(seed, 99)) //nolint:gosec // reproducible test sequence
				rules := make([]ratelimit.Rule, 6)
				for i := range rules {
					rules[i] = ratelimit.Rule{
						Rate:  1 + rng.IntN(50),
						Per:   time.Duration(50+rng.IntN(3000))*time.Millisecond + time.Duration(rng.IntN(1000))*time.Nanosecond,
						Burst: rng.IntN(8),
					}
				}
				rules[0] = ratelimit.Rule{Rate: 3, Per: time.Second, Burst: 5} // interval not exact in seconds
				ctx := context.Background()
				for i := range 1500 {
					k := rng.IntN(len(rules))
					key := fmt.Sprintf("diff:%d:%d", seed, k)
					time.Sleep(time.Duration(rng.IntN(300_000)) * time.Microsecond)
					mr.SetTime(base.Add(time.Since(start)))
					dl, err := local.Allow(ctx, key, rules[k])
					if err != nil {
						t.Fatal(err)
					}
					dr, err := remote.Allow(ctx, key, rules[k])
					if err != nil {
						t.Fatal(err)
					}
					if dl != dr {
						t.Fatalf("request %d, rule %+v:\nlocal %+v\nredis %+v", i, rules[k], dl, dr)
					}
				}
			})
		})
	}
}

// The bucket state is stored as an exact integer string of microseconds, so
// no digits are lost (tostring() on real Redis keeps 14 significant digits).
func TestStoredStateIsExactInteger(t *testing.T) {
	s, mr := newStore(t)
	mr.SetTime(time.Date(2026, 11, 24, 1, 2, 3, 456789000, time.UTC))
	if _, _, _, _, err := s.GCRA(context.Background(), "k", 1_000_000, 1, time.Second); err != nil {
		t.Fatal(err)
	}
	v, err := mr.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := strconv.ParseInt(v, 10, 64); err != nil {
		t.Fatalf("stored TAT %q is not an integer", v)
	}
}
