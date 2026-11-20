// Package cache is a generic two-tier read-through cache.
//
// Tier L1 is an optional in-process cache (otter v2, size-bounded, short TTL).
// Tier L2 is an optional Redis (any [redis.UniversalClient]: standalone,
// Cluster or Sentinel). [Cache.GetOrLoad] collapses concurrent loads of the
// same key in one process (singleflight), so a cold key costs one loader call
// per pod instead of one per request.
//
// L2 failures are fail-open: GetOrLoad logs them, counts them and falls back
// to the loader. L1 entries on other pods are invalidated only by their TTL,
// so keep [LocalConfig.TTL] short for data that changes.
//
// Values are stored in Redis as a one-byte tag followed by the codec output;
// the format is internal to this package.
package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/maypok86/otter/v2"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/singleflight"
)

// ErrNotFound reports a cache miss from [Cache.Get]. Loaders return it (or an
// error wrapping it) to signal that the key does not exist; with
// [Config.NegativeTTL] > 0 that answer is cached too.
var ErrNotFound = errors.New("cache: not found")

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
}

// Config configures a [Cache]. At least one of Redis and Local must be set.
type Config[V any] struct {
	// Redis is the L2 client; nil disables L2.
	Redis redis.UniversalClient
	// Local configures L1; nil disables L1.
	Local *LocalConfig
	// Prefix is prepended to every Redis key, e.g. "user:v1:". Required with Redis.
	Prefix string
	// TTL is the Redis expiry. Required with Redis.
	TTL time.Duration
	// NegativeTTL caches ErrNotFound answers from loaders for this long.
	// Zero disables negative caching.
	NegativeTTL time.Duration
	// Jitter randomises TTLs by ±Jitter (fraction) to avoid synchronized
	// expiry. Zero means the default 0.1; a negative value disables jitter.
	Jitter float64
	// Codec encodes values for Redis. Default: JSON.
	Codec Codec[V]
	// Logger receives L2 failures. Default: slog.Default().
	Logger *slog.Logger
	// MeterProvider for the kitchen.cache.requests counter. Default: global.
	MeterProvider metric.MeterProvider
	// Name labels metrics (attribute cache.name). Default: Prefix.
	Name string
}

type entry[V any] struct {
	v        V
	negative bool
}

// Cache is a typed cache with string keys. It is safe for concurrent use.
type Cache[V any] struct {
	cfg      Config[V]
	l1       *otter.Cache[string, entry[V]]
	sf       singleflight.Group
	log      *slog.Logger
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
	if cfg.NegativeTTL < 0 {
		return nil, errors.New("cache: NegativeTTL must be >= 0")
	}
	if cfg.Jitter == 0 {
		cfg.Jitter = 0.1
	}
	if cfg.Jitter >= 1 {
		return nil, errors.New("cache: Jitter must be < 1")
	}
	if cfg.Codec == nil {
		cfg.Codec = JSON[V]{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
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
		l1, err := otter.New(&otter.Options[string, entry[V]]{
			MaximumSize: lc.MaxSize,
			ExpiryCalculator: otter.ExpiryWritingFunc(func(e otter.Entry[string, entry[V]]) time.Duration {
				if e.Value.negative && cfg.NegativeTTL > 0 {
					return min(lc.TTL, cfg.NegativeTTL)
				}
				return lc.TTL
			}),
		})
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

func (c *Cache[V]) jitter(ttl time.Duration) time.Duration {
	if c.cfg.Jitter <= 0 {
		return ttl
	}
	f := 1 + c.cfg.Jitter*(2*rand.Float64()-1) //nolint:gosec // jitter, not security.
	return time.Duration(float64(ttl) * f)
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

// lookup checks L1 then L2. found=false with err=nil is a clean miss.
// L2 hits are promoted to L1.
func (c *Cache[V]) lookup(ctx context.Context, key string) (e entry[V], found bool, err error) {
	if c.l1 != nil {
		if e, ok := c.l1.GetIfPresent(key); ok {
			c.record(ctx, "l1", "hit")
			return e, true, nil
		}
		c.record(ctx, "l1", "miss")
	}
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
	if c.l1 != nil {
		c.l1.Set(key, e)
	}
	return e, true, nil
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
	return e.v, nil
}

func (c *Cache[V]) store(ctx context.Context, key string, e entry[V]) error {
	ttl := c.cfg.TTL
	if e.negative {
		ttl = c.cfg.NegativeTTL
	}
	if c.l1 != nil {
		c.l1.Set(key, e)
	}
	if c.cfg.Redis == nil {
		return nil
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

// Set stores v under key in every configured tier. L1 is updated even when
// the Redis write fails; the Redis error is returned.
func (c *Cache[V]) Set(ctx context.Context, key string, v V) error {
	return c.store(ctx, key, entry[V]{v: v})
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

// GetOrLoad returns the cached value for key or calls load, caches its result
// and returns it. Concurrent callers for the same key share one load call.
//
// load runs with a context that is not cancelled when one caller gives up
// (values are preserved), so a slow load still fills the cache for the
// others; callers themselves return as soon as their ctx is done.
//
// If load returns ErrNotFound (or wraps it) and NegativeTTL > 0, that answer
// is cached. Other load errors are returned and not cached.
func (c *Cache[V]) GetOrLoad(ctx context.Context, key string, load func(ctx context.Context, key string) (V, error)) (V, error) {
	var zero V
	e, found, err := c.lookup(ctx, key)
	if err != nil {
		c.log.WarnContext(ctx, "cache: lookup failed, loading from source", "cache", c.cfg.Name, "key", key, "error", err)
	}
	if found {
		if e.negative {
			return zero, ErrNotFound
		}
		return e.v, nil
	}

	ch := c.sf.DoChan(key, func() (any, error) {
		lctx := context.WithoutCancel(ctx)
		v, err := load(lctx, key)
		if err != nil {
			c.record(lctx, "loader", "error")
			if errors.Is(err, ErrNotFound) && c.cfg.NegativeTTL > 0 {
				if serr := c.store(lctx, key, entry[V]{negative: true}); serr != nil {
					c.log.WarnContext(lctx, "cache: store negative entry failed", "cache", c.cfg.Name, "key", key, "error", serr)
				}
			}
			return nil, err
		}
		c.record(lctx, "loader", "ok")
		if serr := c.store(lctx, key, entry[V]{v: v}); serr != nil {
			c.log.WarnContext(lctx, "cache: store failed", "cache", c.cfg.Name, "key", key, "error", serr)
		}
		return v, nil
	})
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return zero, r.Err
		}
		return r.Val.(V), nil
	}
}

// GetMany returns the cached values among keys; missing and negative entries
// are absent from the map. Redis lookups are pipelined.
func (c *Cache[V]) GetMany(ctx context.Context, keys []string) (map[string]V, error) {
	out, _, err := c.getMany(ctx, keys)
	return out, err
}

// getMany returns values, the keys still missing (neither value nor negative
// entry), and an L2 error if any (in which case missing holds the L2 keys).
func (c *Cache[V]) getMany(ctx context.Context, keys []string) (map[string]V, []string, error) {
	out := make(map[string]V, len(keys))
	var rest []string
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		if c.l1 != nil {
			if e, ok := c.l1.GetIfPresent(k); ok {
				c.record(ctx, "l1", "hit")
				if !e.negative {
					out[k] = e.v
				}
				continue
			}
			c.record(ctx, "l1", "miss")
		}
		rest = append(rest, k)
	}
	if c.cfg.Redis == nil || len(rest) == 0 {
		return out, rest, nil
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
		return out, rest, fmt.Errorf("cache: redis pipeline get: %w", err)
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
		if c.l1 != nil {
			c.l1.Set(k, e)
		}
		if !e.negative {
			out[k] = e.v
		}
	}
	return out, missing, nil
}

// GetOrLoadMany returns values for keys, calling load once with the keys that
// are not cached. Keys that load omits are treated as not found (and cached
// negatively when NegativeTTL > 0). Batch loads are not deduplicated across
// concurrent callers. Redis failures fail open, as in GetOrLoad.
func (c *Cache[V]) GetOrLoadMany(ctx context.Context, keys []string, load func(ctx context.Context, missing []string) (map[string]V, error)) (map[string]V, error) {
	out, missing, err := c.getMany(ctx, keys)
	if err != nil {
		c.log.WarnContext(ctx, "cache: batch lookup failed, loading from source", "cache", c.cfg.Name, "error", err)
	}
	if len(missing) == 0 {
		return out, nil
	}
	loaded, err := load(ctx, missing)
	if err != nil {
		c.record(ctx, "loader", "error")
		return nil, err
	}
	c.record(ctx, "loader", "ok")
	for _, k := range missing {
		v, ok := loaded[k]
		var serr error
		switch {
		case ok:
			out[k] = v
			serr = c.store(ctx, k, entry[V]{v: v})
		case c.cfg.NegativeTTL > 0:
			serr = c.store(ctx, k, entry[V]{negative: true})
		}
		if serr != nil {
			c.log.WarnContext(ctx, "cache: store failed", "cache", c.cfg.Name, "key", k, "error", serr)
		}
	}
	return out, nil
}

// DeleteByPrefix removes every key starting with prefix (relative to
// Config.Prefix) from L1 (this pod) and Redis, using SCAN + UNLINK on every
// master. It returns the number of Redis keys removed. It is O(keyspace);
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
	pattern := globEscape(c.redisKey(prefix)) + "*"
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
	var err error
	if cc, ok := c.cfg.Redis.(*redis.ClusterClient); ok {
		// ForEachMaster runs concurrently; serialise the counter.
		var mu sync.Mutex
		err = cc.ForEachMaster(ctx, func(ctx context.Context, cl *redis.Client) error {
			mu.Lock()
			defer mu.Unlock()
			return scan(ctx, cl)
		})
	} else {
		err = scan(ctx, c.cfg.Redis)
	}
	if err != nil {
		return total, fmt.Errorf("cache: delete by prefix: %w", err)
	}
	return total, nil
}

// globEscape escapes Redis glob metacharacters.
func globEscape(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '*', '?', '[', ']', '\\', '^', '-':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}
