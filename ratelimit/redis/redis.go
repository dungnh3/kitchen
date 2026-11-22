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
	"strconv"
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
	if burst == 0 {
		burst = rate
	}
	if rate <= 0 || per <= 0 || burst < 0 {
		return false, 0, 0, 0, fmt.Errorf("ratelimit/redis: invalid rule rate=%d burst=%d per=%s", rate, burst, per)
	}
	res, err := gcra.Run(ctx, s.rdb, []string{key},
		burst, rate, strconv.FormatFloat(per.Seconds(), 'f', -1, 64), 1).Slice()
	if err != nil {
		return false, 0, 0, 0, err
	}
	return parse(res)
}

func parse(res []any) (allowed bool, remaining int, retryAfter, resetAfter time.Duration, err error) {
	bad := fmt.Errorf("ratelimit/redis: unexpected reply %v", res)
	if len(res) != 4 {
		return false, 0, 0, 0, bad
	}
	a, ok1 := res[0].(int64)
	rem, ok2 := res[1].(int64)
	retry, err1 := seconds(res[2])
	reset, err2 := seconds(res[3])
	if !ok1 || !ok2 || err1 != nil || err2 != nil {
		return false, 0, 0, 0, bad
	}
	return a == 1, int(rem), retry, reset, nil
}

func seconds(v any) (time.Duration, error) {
	s, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("not a string: %T", v)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	return time.Duration(f * float64(time.Second)), nil
}
