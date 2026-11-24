// Package redis is a shared GCRA backend for kitchen/ratelimit, so every
// instance of a service uses the same buckets. It runs an embedded GCRA Lua
// script with EVALSHA (falling back to EVAL on NOSCRIPT) and works with any
// go-redis UniversalClient: standalone, Sentinel or Cluster.
//
// Plug it in with ratelimit.RemoteStore:
//
//	store, err := redis.New(rdb)
//	limiter, err := ratelimit.New(ratelimit.Config{Store: ratelimit.RemoteStore(store), ...})
//
// Circuit breaking, timeouts and the behavior during a Redis outage live in
// ratelimit.Limiter; this package only talks to Redis. It implements
// ratelimit.Remote with plain types and does not import the core module.
package redis

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

//go:embed gcra.lua
var gcraLua string

var gcra = goredis.NewScript(gcraLua)

// Store implements ratelimit.Remote.
type Store struct {
	rdb goredis.UniversalClient
}

// New returns a Store using rdb, which the caller owns and closes.
//
// rdb must have ContextTimeoutEnabled set: otherwise go-redis ignores
// context deadlines and the limiter's per-call timeout (100ms by default)
// cannot protect requests from a hanging Redis. Clients from redisx.Open
// (package github.com/dungnh3/kitchen/redisx, traced and metered) have it.
func New(rdb goredis.UniversalClient) (*Store, error) {
	if rdb == nil {
		return nil, errors.New("ratelimit/redis: nil client")
	}
	if !honorsContextTimeout(rdb) {
		return nil, errors.New("ratelimit/redis: client must have ContextTimeoutEnabled set")
	}
	return &Store{rdb: rdb}, nil
}

func honorsContextTimeout(rdb goredis.UniversalClient) bool {
	switch c := rdb.(type) {
	case *goredis.Client:
		return c.Options().ContextTimeoutEnabled
	case *goredis.ClusterClient:
		return c.Options().ContextTimeoutEnabled
	case *goredis.Ring:
		return c.Options().ContextTimeoutEnabled
	}
	return false
}

// GCRA consumes one request from bucket key: rate requests per per, bursts
// up to burst (0 means rate). It implements ratelimit.Remote.
func (s *Store) GCRA(ctx context.Context, key string, rate, burst int, per time.Duration) (allowed bool, remaining int, retryAfter, resetAfter time.Duration, err error) {
	interval, tolerance, err := gcraParams(rate, burst, per)
	if err != nil {
		return false, 0, 0, 0, err
	}
	res, err := gcra.Run(ctx, s.rdb, []string{key}, interval, tolerance).Slice()
	if err != nil {
		return false, 0, 0, 0, err
	}
	return parse(res)
}

// maxTolerance keeps the bucket state an exact integer in a Lua number.
const maxTolerance = 1 << 52

// gcraParams returns the emission interval and burst tolerance in
// microseconds, the interval rounded up. It must stay identical to the one
// in package ratelimit (LocalStore); the differential test checks it.
func gcraParams(rate, burst int, per time.Duration) (interval, tolerance int64, err error) {
	if burst == 0 {
		burst = rate
	}
	if rate <= 0 || per <= 0 || burst < 0 || int64(per/time.Microsecond) < int64(rate) {
		return 0, 0, fmt.Errorf("ratelimit/redis: invalid rule rate=%d burst=%d per=%s", rate, burst, per)
	}
	us := int64(per / time.Microsecond)
	if per%time.Microsecond != 0 {
		us++
	}
	interval = (us + int64(rate) - 1) / int64(rate)
	if interval > maxTolerance/int64(burst) {
		return 0, 0, fmt.Errorf("ratelimit/redis: burst %d x %dus is too long", burst, interval)
	}
	return interval, interval * int64(burst), nil
}

func parse(res []any) (allowed bool, remaining int, retryAfter, resetAfter time.Duration, err error) {
	if len(res) != 4 {
		return false, 0, 0, 0, fmt.Errorf("ratelimit/redis: unexpected reply %v", res)
	}
	var v [4]int64
	for i, x := range res {
		n, ok := x.(int64)
		if !ok {
			return false, 0, 0, 0, fmt.Errorf("ratelimit/redis: unexpected reply %v", res)
		}
		v[i] = n
	}
	return v[0] == 1, int(v[1]), time.Duration(v[2]) * time.Microsecond, time.Duration(v[3]) * time.Microsecond, nil
}
