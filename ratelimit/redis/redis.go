// Package redis is a ratelimit.Store backed by Redis, so every instance of
// a service shares the same buckets. It runs an embedded GCRA Lua script
// with EVALSHA (falling back to EVAL on NOSCRIPT) and works with any
// go-redis UniversalClient: standalone, Sentinel or Cluster.
//
// Circuit breaking, timeouts and the behavior during a Redis outage live in
// ratelimit.Limiter; this package only talks to Redis.
package redis

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dungnh3/kitchen/ratelimit"
	goredis "github.com/redis/go-redis/v9"
)

//go:embed gcra.lua
var gcraLua string

var gcra = goredis.NewScript(gcraLua)

// Store implements ratelimit.Store.
type Store struct {
	rdb goredis.UniversalClient
}

var _ ratelimit.Store = (*Store)(nil)

// New returns a Store using rdb, which the caller owns and closes.
//
// rdb must have ContextTimeoutEnabled set: otherwise go-redis ignores
// context deadlines and the limiter's per-call timeout (100ms by default)
// cannot protect requests from a hanging Redis.
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

// Allow implements ratelimit.Store.
func (s *Store) Allow(ctx context.Context, key string, r ratelimit.Rule) (ratelimit.Decision, error) {
	burst := r.Burst
	if burst == 0 {
		burst = r.Rate
	}
	if r.Rate <= 0 || r.Per <= 0 {
		return ratelimit.Decision{Allowed: true}, nil
	}
	res, err := gcra.Run(ctx, s.rdb, []string{key},
		burst, r.Rate, strconv.FormatFloat(r.Per.Seconds(), 'f', -1, 64), 1).Slice()
	if err != nil {
		return ratelimit.Decision{}, err
	}
	return parse(res, r.Rate)
}

func parse(res []any, limit int) (ratelimit.Decision, error) {
	if len(res) != 4 {
		return ratelimit.Decision{}, fmt.Errorf("ratelimit/redis: unexpected reply %v", res)
	}
	allowed, ok1 := res[0].(int64)
	remaining, ok2 := res[1].(int64)
	retry, err1 := seconds(res[2])
	reset, err2 := seconds(res[3])
	if !ok1 || !ok2 || err1 != nil || err2 != nil {
		return ratelimit.Decision{}, fmt.Errorf("ratelimit/redis: unexpected reply %v", res)
	}
	return ratelimit.Decision{
		Allowed:    allowed == 1,
		Limit:      limit,
		Remaining:  int(remaining),
		RetryAfter: retry,
		ResetAfter: reset,
	}, nil
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
