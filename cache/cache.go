// Package cache is a generic two-tier read-through cache.
//
// Tier L1 is an optional in-process cache (otter v2, size-bounded, short TTL,
// optional refresh-ahead). Tier L2 is an optional Redis (any
// [redis.UniversalClient]: standalone, Cluster or Sentinel; open it with
// kitchen/redisx). [Cache.GetOrLoad] and [Cache.GetOrLoadMany] collapse
// concurrent loads of the same key in one process, so a cold key costs one
// loader call per pod instead of one per request.
//
// L2 failures are fail-open: loads log them, count them and fall back to the
// loader. L1 entries on other pods are invalidated only by their TTL, so keep
// [LocalConfig.TTL] short for data that changes, or set
// [LocalConfig.RefreshAfter] to refresh hot keys in the background.
//
// Values handed out from L1 are shared between callers. For pointer, slice
// or map values set [Config.Clone], or never mutate what the cache returns.
//
// Values are stored in Redis as a one-byte tag followed by the codec output;
// the format is internal to this package.
package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/singleflight"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/redisx"
	"github.com/dungnh3/kitchen/timex"
)

// ErrNotFound reports a cache miss from [Cache.Get]. It is errs.ErrNotFound,
// so errors.Is(err, ErrNotFound) matches every errs error with code NotFound:
// a loader returning any of them (for example a repository's
// "user not found") signals a missing key, cached for [Config.NegativeTTL].
var ErrNotFound = errs.ErrNotFound

// DefaultLoadTimeout bounds a loader call when Config.LoadTimeout is zero.
const DefaultLoadTimeout = 30 * time.Second

const (
	tagValue    byte = 'v'
	tagNegative byte = 'n'

	scopeName = "github.com/dungnh3/kitchen/cache"
)

// LocalConfig configures the in-process L1 tier.
type LocalConfig struct {
	// MaxSize is the maximum number of entries. Required (> 0).
	MaxSize int
	// TTL is how long an entry lives in L1 after it is written. Required.
	TTL time.Duration
	// RefreshAfter, when set (< TTL), reloads an entry in the background
	// (from L2, else the loader) on the first GetOrLoad after it is this old,
	// while callers keep getting the current value: hot keys never expire
	// under load. Zero disables it.
	RefreshAfter time.Duration
}

// Config configures a [Cache]. At least one of Redis and Local must be set.
type Config[V any] struct {
	// Redis is the L2 client; nil disables L2.
	Redis redis.UniversalClient
	// Local configures L1; nil disables L1.
	Local *LocalConfig
	// Prefix is prepended to every Redis key, e.g. "user:v1:". Required with Redis.
	Prefix string
	// TTL is the default Redis expiry. Required with Redis.
	TTL time.Duration
	// NegativeTTL caches not-found answers from loaders for this long.
	// Zero disables negative caching.
	NegativeTTL time.Duration
	// Jitter randomises Redis TTLs by ±Jitter (fraction) to avoid
	// synchronized expiry. Zero means the default 0.1; negative disables it.
	Jitter float64
	// LoadTimeout bounds each loader call, which runs detached from the
	// caller that started it. Default DefaultLoadTimeout.
	LoadTimeout time.Duration
	// Clone, when set, copies values going into and out of the cache, so
	// callers cannot mutate shared entries. Use it for pointer, slice and
	// map values.
	Clone func(V) V
	// Codec encodes values for Redis. Default: JSON.
	Codec Codec[V]
	// Logger receives L2 failures. Default: log.Default().
	Logger *log.Logger
	// MeterProvider for the kitchen.cache.requests counter. Default: global.
	MeterProvider metric.MeterProvider
	// Name labels metrics (attribute cache.name). Default: Prefix.
	Name string
}

type entry[V any] struct {
	v        V
	negative bool
	ttl      time.Duration // per-entry TTL from WithTTL; 0 = default
}

// Cache is a typed cache with string keys. It is safe for concurrent use.
type Cache[V any] struct {
	cfg      Config[V]
	l1       *otter.Cache[string, entry[V]]
	sf       singleflight.Group
	log      *log.Logger
	requests metric.Int64Counter
	nameAttr attribute.KeyValue
}

// New validates cfg and returns a cache. Call [Cache.Close] when done.
func New[V any](cfg Config[V]) (*Cache[V], error) {
	if cfg.Redis == nil && cfg.Local == nil {
		return nil, errors.New("cache: at least one of Redis or Local is required")
	}
	if cfg.Redis != nil {
		if cfg.Prefix == "" {
			return nil, errors.New("cache: Prefix is required with Redis")
		}
		if cfg.TTL <= 0 {
			return nil, errors.New("cache: TTL must be > 0 with Redis")
		}
	}
	if cfg.NegativeTTL < 0 || cfg.LoadTimeout < 0 {
		return nil, errors.New("cache: NegativeTTL and LoadTimeout must be >= 0")
	}
	if cfg.Jitter == 0 {
		cfg.Jitter = 0.1
	}
	if cfg.Jitter >= 1 {
		return nil, errors.New("cache: Jitter must be < 1")
	}
	if cfg.LoadTimeout == 0 {
		cfg.LoadTimeout = DefaultLoadTimeout
	}
	if cfg.Codec == nil {
		cfg.Codec = JSON[V]{}
	}
	cfg.Logger = log.OrDefault(cfg.Logger)
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	if cfg.Name == "" {
		cfg.Name = cfg.Prefix
	}
	c := &Cache[V]{cfg: cfg, log: cfg.Logger, nameAttr: attribute.String("cache.name", cfg.Name)}

	if lc := cfg.Local; lc != nil {
		if lc.MaxSize <= 0 || lc.TTL <= 0 {
			return nil, errors.New("cache: Local.MaxSize and Local.TTL must be > 0")
		}
		if lc.RefreshAfter < 0 || lc.RefreshAfter > 0 && lc.RefreshAfter >= lc.TTL {
			return nil, errors.New("cache: Local.RefreshAfter must be in [0, Local.TTL)")
		}
		opts := &otter.Options[string, entry[V]]{
			MaximumSize: lc.MaxSize,
			ExpiryCalculator: otter.ExpiryWritingFunc(func(e otter.Entry[string, entry[V]]) time.Duration {
				ttl := lc.TTL
				if e.Value.negative && cfg.NegativeTTL > 0 {
					ttl = min(ttl, cfg.NegativeTTL)
				}
				if e.Value.ttl > 0 {
					ttl = min(ttl, e.Value.ttl)
				}
				return ttl
			}),
		}
		if lc.RefreshAfter > 0 {
			opts.RefreshCalculator = otter.RefreshWriting[string, entry[V]](lc.RefreshAfter)
		}
		l1, err := otter.New(opts)
		if err != nil {
			return nil, fmt.Errorf("cache: local tier: %w", err)
		}
		c.l1 = l1
	}

	var err error
	c.requests, err = cfg.MeterProvider.Meter(scopeName).Int64Counter("kitchen.cache.requests",
		metric.WithDescription("Cache lookups by tier and result."),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("cache: metric: %w", err)
	}
	return c, nil
}

// Close stops the L1 background goroutines. The cache must not be used after.
// It does not close the Redis client.
func (c *Cache[V]) Close() {
	if c.l1 != nil {
		c.l1.StopAllGoroutines()
	}
}

func (c *Cache[V]) record(ctx context.Context, tier, result string) {
	c.requests.Add(ctx, 1, metric.WithAttributes(c.nameAttr,
		attribute.String("cache.tier", tier), attribute.String("cache.result", result)))
}

func (c *Cache[V]) redisKey(key string) string { return c.cfg.Prefix + key }

func (c *Cache[V]) clone(v V) V {
	if c.cfg.Clone == nil {
		return v
	}
	return c.cfg.Clone(v)
}

func (c *Cache[V]) jitter(ttl time.Duration) time.Duration {
	return timex.Jitter(ttl, c.cfg.Jitter)
}

func (c *Cache[V]) encode(e entry[V]) ([]byte, error) {
	if e.negative {
		return []byte{tagNegative}, nil
	}
	b, err := c.cfg.Codec.Marshal(e.v)
	if err != nil {
		return nil, fmt.Errorf("cache: encode: %w", err)
	}
	return append([]byte{tagValue}, b...), nil
}

func (c *Cache[V]) decode(b []byte) (entry[V], error) {
	var e entry[V]
	if len(b) == 0 {
		return e, errors.New("cache: decode: empty payload")
	}
	switch b[0] {
	case tagNegative:
		e.negative = true
		return e, nil
	case tagValue:
		if err := c.cfg.Codec.Unmarshal(b[1:], &e.v); err != nil {
			return e, fmt.Errorf("cache: decode: %w", err)
		}
		return e, nil
	default:
		return e, fmt.Errorf("cache: decode: unknown tag %q", b[0])
	}
}

// lookupL2 reads key from Redis. found=false with err=nil is a clean miss.
func (c *Cache[V]) lookupL2(ctx context.Context, key string) (e entry[V], found bool, err error) {
	if c.cfg.Redis == nil {
		return e, false, nil
	}
	b, err := c.cfg.Redis.Get(ctx, c.redisKey(key)).Bytes()
	switch {
	case errors.Is(err, redis.Nil):
		c.record(ctx, "l2", "miss")
		return e, false, nil
	case err != nil:
		c.record(ctx, "l2", "error")
		return e, false, fmt.Errorf("cache: redis get %q: %w", key, err)
	}
	e, err = c.decode(b)
	if err != nil {
		c.record(ctx, "l2", "error")
		return e, false, err
	}
	c.record(ctx, "l2", "hit")
	return e, true, nil
}

// lookup checks L1 then L2; L2 hits are promoted to L1.
func (c *Cache[V]) lookup(ctx context.Context, key string) (e entry[V], found bool, err error) {
	if c.l1 != nil {
		if e, ok := c.l1.GetIfPresent(key); ok {
			c.record(ctx, "l1", "hit")
			return e, true, nil
		}
		c.record(ctx, "l1", "miss")
	}
	e, found, err = c.lookupL2(ctx, key)
	if found && c.l1 != nil {
		c.l1.Set(key, e)
	}
	return e, found, err
}

// Get returns the cached value for key. A miss, or a cached negative answer,
// returns ErrNotFound. Redis errors are returned (Get does not fail open).
func (c *Cache[V]) Get(ctx context.Context, key string) (V, error) {
	var zero V
	e, found, err := c.lookup(ctx, key)
	if err != nil {
		return zero, err
	}
	if !found || e.negative {
		return zero, ErrNotFound
	}
	return c.clone(e.v), nil
}

func (c *Cache[V]) storeL2(ctx context.Context, key string, e entry[V]) error {
	if c.cfg.Redis == nil {
		return nil
	}
	ttl := c.cfg.TTL
	switch {
	case e.negative:
		ttl = c.cfg.NegativeTTL
	case e.ttl > 0:
		ttl = e.ttl
	}
	b, err := c.encode(e)
	if err != nil {
		return err
	}
	if err := c.cfg.Redis.Set(ctx, c.redisKey(key), b, c.jitter(ttl)).Err(); err != nil {
		return fmt.Errorf("cache: redis set %q: %w", key, err)
	}
	return nil
}

func (c *Cache[V]) store(ctx context.Context, key string, e entry[V]) error {
	if c.l1 != nil {
		c.l1.Set(key, e)
	}
	return c.storeL2(ctx, key, e)
}

// SetOption configures Set.
type SetOption func(*setOpts)

type setOpts struct{ ttl time.Duration }

// WithTTL overrides the TTL of one entry: Redis uses it instead of
// Config.TTL, L1 keeps the entry for at most min(TTL, Local.TTL).
func WithTTL(d time.Duration) SetOption { return func(o *setOpts) { o.ttl = d } }

// Set stores v under key in every configured tier. L1 is updated even when
// the Redis write fails; the Redis error is returned.
func (c *Cache[V]) Set(ctx context.Context, key string, v V, opts ...SetOption) error {
	var o setOpts
	for _, opt := range opts {
		opt(&o)
	}
	if o.ttl < 0 {
		return errors.New("cache: negative TTL")
	}
	return c.store(ctx, key, entry[V]{v: c.clone(v), ttl: o.ttl})
}

// Delete removes keys from L1 (this pod only) and Redis.
func (c *Cache[V]) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if c.l1 != nil {
		for _, k := range keys {
			c.l1.Invalidate(k)
		}
	}
	if c.cfg.Redis == nil {
		return nil
	}
	// One DEL per key keeps Redis Cluster happy (no CROSSSLOT).
	_, err := c.cfg.Redis.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, k := range keys {
			p.Del(ctx, c.redisKey(k))
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("cache: redis del: %w", err)
	}
	return nil
}

// PanicError is returned when a loader panics; the panic does not crash the
// process.
type PanicError struct{ Value any }

func (p *PanicError) Error() string { return fmt.Sprintf("cache: loader panicked: %v", p.Value) }

// callLoad runs load with the load timeout and turns a panic into an error.
func callLoad[R any](ctx context.Context, timeout time.Duration, load func(context.Context) (R, error)) (r R, err error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	defer func() {
		if p := recover(); p != nil {
			err = &PanicError{Value: p}
		}
	}()
	return load(ctx)
}

// flight loads key once per process: L2 first, then load. It runs detached
// from the caller (bounded by LoadTimeout); the caller waits until its own
// ctx is done. A value loaded from the source is written to both tiers, so
// it is kept even when every caller gave up.
func (c *Cache[V]) flight(ctx context.Context, key string, load func(ctx context.Context, key string) (V, error)) (entry[V], error) {
	ch := c.sf.DoChan(key, func() (any, error) {
		lctx := context.WithoutCancel(ctx)
		e, found, err := c.lookupL2(lctx, key)
		if err != nil {
			c.log.WarnContext(lctx, "cache: lookup failed, loading from source", log.String("cache", c.cfg.Name), log.String("key", key), log.Err(err))
		}
		if found {
			return e, nil
		}
		v, err := callLoad(lctx, c.cfg.LoadTimeout, func(ctx context.Context) (V, error) { return load(ctx, key) })
		if err != nil {
			c.record(lctx, "loader", "error")
			if errors.Is(err, ErrNotFound) && c.cfg.NegativeTTL > 0 {
				ne := entry[V]{negative: true}
				if serr := c.store(lctx, key, ne); serr != nil {
					c.log.WarnContext(lctx, "cache: store negative entry failed", log.String("cache", c.cfg.Name), log.String("key", key), log.Err(serr))
				}
				return ne, nil
			}
			return nil, err
		}
		c.record(lctx, "loader", "ok")
		e = entry[V]{v: v}
		if serr := c.store(lctx, key, e); serr != nil {
			c.log.WarnContext(lctx, "cache: store failed", log.String("cache", c.cfg.Name), log.String("key", key), log.Err(serr))
		}
		return e, nil
	})
	select {
	case <-ctx.Done():
		return entry[V]{}, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return entry[V]{}, r.Err
		}
		return r.Val.(entry[V]), nil
	}
}

// GetOrLoad returns the cached value for key or calls load, caches its result
// and returns it. Concurrent callers for the same key share one load call.
//
// load runs detached from the caller that started it, bounded by
// Config.LoadTimeout, so a slow load still fills the cache for the others;
// callers themselves return as soon as their ctx is done. A panic in load is
// returned as a *PanicError.
//
// If load returns an error matching ErrNotFound and NegativeTTL > 0, that
// answer is cached. Other load errors are returned and not cached.
func (c *Cache[V]) GetOrLoad(ctx context.Context, key string, load func(ctx context.Context, key string) (V, error)) (V, error) {
	var zero V
	var e entry[V]
	var err error
	if c.l1 != nil {
		var missed atomic.Bool // also set by background refreshes
		e, err = c.l1.Get(ctx, key, otter.LoaderFunc[string, entry[V]](func(ctx context.Context, key string) (entry[V], error) {
			missed.Store(true)
			return c.flight(ctx, key, load)
		}))
		if !missed.Load() {
			c.record(ctx, "l1", "hit")
		} else {
			c.record(ctx, "l1", "miss")
		}
	} else {
		e, err = c.flight(ctx, key, load)
	}
	if err != nil {
		return zero, err
	}
	if e.negative {
		return zero, ErrNotFound
	}
	return c.clone(e.v), nil
}

// GetMany returns the cached values among keys; missing and negative entries
// are absent from the map. Redis lookups are pipelined.
func (c *Cache[V]) GetMany(ctx context.Context, keys []string) (map[string]V, error) {
	found, _, err := c.getMany(ctx, keys, true)
	out := make(map[string]V, len(found))
	for k, e := range found {
		if !e.negative {
			out[k] = c.clone(e.v)
		}
	}
	return out, err
}

// getMany returns the entries found (negative ones included), the keys still
// missing, and an L2 error if any (in which case missing holds the L2 keys).
func (c *Cache[V]) getMany(ctx context.Context, keys []string, useL1 bool) (map[string]entry[V], []string, error) {
	found := make(map[string]entry[V], len(keys))
	var rest []string
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		if useL1 && c.l1 != nil {
			if e, ok := c.l1.GetIfPresent(k); ok {
				c.record(ctx, "l1", "hit")
				found[k] = e
				continue
			}
			c.record(ctx, "l1", "miss")
		}
		rest = append(rest, k)
	}
	if c.cfg.Redis == nil || len(rest) == 0 {
		return found, rest, nil
	}
	cmds := make([]*redis.StringCmd, len(rest))
	_, err := c.cfg.Redis.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, k := range rest {
			cmds[i] = p.Get(ctx, c.redisKey(k))
		}
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		c.record(ctx, "l2", "error")
		return found, rest, fmt.Errorf("cache: redis pipeline get: %w", err)
	}
	var missing []string
	for i, k := range rest {
		b, err := cmds[i].Bytes()
		if err != nil {
			c.record(ctx, "l2", "miss")
			missing = append(missing, k)
			continue
		}
		e, err := c.decode(b)
		if err != nil {
			c.record(ctx, "l2", "error")
			missing = append(missing, k)
			continue
		}
		c.record(ctx, "l2", "hit")
		if useL1 && c.l1 != nil {
			c.l1.Set(k, e)
		}
		found[k] = e
	}
	return found, missing, nil
}

// GetOrLoadMany returns values for keys, calling load once with the keys that
// are not cached. Keys that load omits are treated as not found (and cached
// negatively when NegativeTTL > 0). With L1, a key that another call is
// already loading is waited for instead of loaded again (otter BulkGet). Redis failures fail open, as
// in GetOrLoad; load runs with the caller's ctx bounded by LoadTimeout and a
// panic is returned as a *PanicError.
func (c *Cache[V]) GetOrLoadMany(ctx context.Context, keys []string, load func(ctx context.Context, missing []string) (map[string]V, error)) (map[string]V, error) {
	var found map[string]entry[V]
	var err error
	if c.l1 != nil {
		found, err = c.l1.BulkGet(ctx, keys, otter.BulkLoaderFunc[string, entry[V]](func(ctx context.Context, keys []string) (map[string]entry[V], error) {
			return c.loadMany(ctx, keys, load)
		}))
	} else {
		found, err = c.loadMany(ctx, keys, load)
	}
	if err != nil {
		return nil, err
	}
	out := make(map[string]V, len(found))
	for k, e := range found {
		if !e.negative {
			out[k] = c.clone(e.v)
		}
	}
	return out, nil
}

// loadMany reads keys from L2 and loads the rest from the source, storing
// results in L2 (otter commits them to L1).
func (c *Cache[V]) loadMany(ctx context.Context, keys []string, load func(ctx context.Context, missing []string) (map[string]V, error)) (map[string]entry[V], error) {
	found, missing, err := c.getMany(ctx, keys, false)
	if err != nil {
		c.log.WarnContext(ctx, "cache: batch lookup failed, loading from source", log.String("cache", c.cfg.Name), log.Err(err))
	}
	if len(missing) == 0 {
		return found, nil
	}
	loaded, err := callLoad(ctx, c.cfg.LoadTimeout, func(ctx context.Context) (map[string]V, error) { return load(ctx, missing) })
	if err != nil {
		c.record(ctx, "loader", "error")
		return nil, err
	}
	c.record(ctx, "loader", "ok")
	for _, k := range missing {
		v, ok := loaded[k]
		var e entry[V]
		switch {
		case ok:
			e = entry[V]{v: v}
		case c.cfg.NegativeTTL > 0:
			e = entry[V]{negative: true}
		default:
			continue
		}
		found[k] = e
		if serr := c.storeL2(ctx, k, e); serr != nil {
			c.log.WarnContext(ctx, "cache: store failed", log.String("cache", c.cfg.Name), log.String("key", k), log.Err(serr))
		}
	}
	return found, nil
}

// DeleteByPrefix removes every key starting with prefix (relative to
// Config.Prefix) from L1 (this pod) and Redis, using SCAN + UNLINK on every
// shard ([redisx.ForEachShard]). It returns the number of Redis keys removed. It is O(keyspace);
// use it for admin and invalidation jobs, not on hot paths.
func (c *Cache[V]) DeleteByPrefix(ctx context.Context, prefix string) (int, error) {
	if c.l1 != nil {
		for k := range c.l1.Keys() {
			if strings.HasPrefix(k, prefix) {
				c.l1.Invalidate(k)
			}
		}
	}
	if c.cfg.Redis == nil {
		return 0, nil
	}
	pattern := redisx.GlobEscape(c.redisKey(prefix)) + "*"
	var total int
	scan := func(ctx context.Context, cl redis.UniversalClient) error {
		var cursor uint64
		for {
			keys, next, err := cl.Scan(ctx, cursor, pattern, 500).Result()
			if err != nil {
				return err
			}
			for _, k := range keys {
				// UNLINK one key at a time: keys of one SCAN batch may hash
				// to different slots in Cluster mode.
				n, err := cl.Unlink(ctx, k).Result()
				if err != nil {
					return err
				}
				total += int(n)
			}
			if next == 0 {
				return nil
			}
			cursor = next
		}
	}
	if err := redisx.ForEachShard(ctx, c.cfg.Redis, scan); err != nil {
		return total, fmt.Errorf("cache: delete by prefix: %w", err)
	}
	return total, nil
}
