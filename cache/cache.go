// Package cache is a generic two-tier read-through cache.
//
// Tier L1 is an optional in-process cache (otter v2, bounded by entry count
// or by weight, short TTL, optional refresh-ahead). Tier L2 is an optional
// Redis (any [redis.UniversalClient]: standalone, Cluster or Sentinel; open
// it with kitchen/redisx). [Cache.GetOrLoad] and [Cache.GetOrLoadMany]
// collapse concurrent loads of the same key in one process, so a cold key
// costs one loader call per pod instead of one per request.
//
// Consistency: a [Cache.Set] or [Cache.Delete] that runs while a key is being
// loaded wins over the load. In L1 the load's result is dropped; in Redis a
// load only writes back if the key still holds what the load saw when it
// missed (a lease, as in Memcache), so a load that read the source before an
// invalidation cannot write the old value back on any pod. L1 entries on
// other pods are invalidated only by their TTL, so keep [LocalConfig.TTL]
// short for data that changes: it bounds how stale another pod can be.
//
// Redis failures are fail-open on the read-through paths: loads log them
// (at most once per 10s), count them and fall back to the loader. Every Redis
// command is bounded by [Config.RedisTimeout], and after repeated failures the
// read-through paths skip Redis for a few seconds instead of waiting on it.
//
// Values handed out from L1 are shared between callers. For pointer, slice
// or map values set [Config.Clone], or never mutate what the cache returns.
//
// Values are stored in Redis as a one-byte tag followed by the codec output;
// the format is internal to this package.
package cache

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/maphash"
	"strings"
	"sync"
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

// ErrClosed is returned by the loading methods after [Cache.Close].
var ErrClosed = errors.New("cache: closed")

// DefaultLoadTimeout bounds a loader call when Config.LoadTimeout is zero.
const DefaultLoadTimeout = 30 * time.Second

// DefaultRedisTimeout bounds each Redis command when Config.RedisTimeout is
// zero.
const DefaultRedisTimeout = 500 * time.Millisecond

const (
	tagValue    byte = 'v'
	tagNegative byte = 'n'
	tagLease    byte = 'l'

	scopeName = "github.com/dungnh3/kitchen/cache"

	// genStripes is the number of write-generation counters keys hash to.
	genStripes = 64
	// After breakerThreshold consecutive Redis failures the read-through
	// paths skip Redis for breakerCooldown.
	breakerThreshold = 5
	breakerCooldown  = 5 * time.Second
	// warnInterval is the minimum time between two warnings of one kind.
	warnInterval = 10 * time.Second
	// noCacheTTL is the L1 lifetime of a value handed to callers but not
	// kept: it expires as soon as it is written.
	noCacheTTL = time.Nanosecond
)

// acquireScript returns the key's payload and remaining TTL in ms. On a miss
// it stores a lease (ARGV[1], expiring after ARGV[2] ms) and returns it.
var acquireScript = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if v then return {v, redis.call('PTTL', KEYS[1])} end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return {ARGV[1], -2}
`)

// storeScript writes ARGV[2] for ARGV[3] ms only if the key still holds
// ARGV[1], the payload or lease the load saw. It returns 1 if it wrote.
var storeScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
  return 1
end
return 0
`)

// LocalConfig configures the in-process L1 tier.
type LocalConfig struct {
	// MaxSize is the maximum number of entries. Exactly one of MaxSize and
	// MaxWeight must be > 0.
	MaxSize int
	// MaxWeight bounds the sum of entry weights given by Config.Weigher,
	// e.g. bytes. Use it when value sizes vary widely.
	MaxWeight uint64
	// TTL is how long an entry lives in L1 after it is written. Required.
	// Entries live between TTL×(1−Jitter) and TTL, so entries written
	// together do not expire together.
	TTL time.Duration
	// RefreshAfter, when set (< TTL), reloads an entry in the background
	// (from L2, else the loader) on the first GetOrLoad after it is this old,
	// while callers keep getting the current value: hot keys never expire
	// under load. Zero disables it.
	RefreshAfter time.Duration
}

// Config configures a [Cache]. At least one of Redis and Local must be set.
type Config[V any] struct {
	// Redis is the L2 client; nil disables L2. Open it with redisx.Open so
	// context deadlines are honoured.
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
	// Jitter randomises TTLs to avoid synchronized expiry: Redis TTLs by
	// ±Jitter (fraction), L1 TTLs downwards only, by up to Jitter. Zero means
	// the default 0.1; negative disables it.
	Jitter float64
	// LoadTimeout bounds each loader call, which runs detached from the
	// caller that started it. Default DefaultLoadTimeout.
	LoadTimeout time.Duration
	// RedisTimeout bounds each Redis command (or pipeline) of Get, GetMany,
	// Set, Delete, GetOrLoad and GetOrLoadMany. Default DefaultRedisTimeout.
	RedisTimeout time.Duration
	// Clone, when set, copies values going into and out of the cache, so
	// callers cannot mutate shared entries. Use it for pointer, slice and
	// map values.
	Clone func(V) V
	// Weigher gives an entry's weight for Local.MaxWeight (e.g. its size in
	// bytes). Required with MaxWeight, not allowed without it. Negative
	// entries weigh 1.
	Weigher func(key string, v V) uint32
	// Codec encodes values for Redis. Default: JSON.
	Codec Codec[V]
	// Logger receives L2 failures. Default: log.Default().
	Logger *log.Logger
	// MeterProvider for the cache metrics. Default: global.
	MeterProvider metric.MeterProvider
	// Name labels metrics (attribute cache.name) and logs. Default: Prefix.
	// Required when Prefix is empty.
	Name string
}

type entry[V any] struct {
	v        V
	negative bool
	ttl      time.Duration // upper bound on the L1 lifetime; 0 = none
}

// Cache is a typed cache with string keys. It is safe for concurrent use.
type Cache[V any] struct {
	cfg     Config[V]
	l1      *otter.Cache[string, entry[V]]
	refresh bool
	sf      singleflight.Group
	log     *log.Logger
	m       instruments

	// gens and epoch are bumped before every local write to L1 (Set,
	// Delete, DeleteByPrefix); a fill computed before a bump is dropped.
	seed  maphash.Seed
	gens  [genStripes]atomic.Uint64
	epoch atomic.Uint64

	brk       breaker
	warnRead  throttle
	warnWrite throttle

	base   context.Context // cancelled by Close
	stop   context.CancelFunc
	track  tracker
	closed atomic.Bool
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
	if cfg.Name == "" {
		if cfg.Prefix == "" {
			return nil, errors.New("cache: Name is required when Prefix is empty")
		}
		cfg.Name = cfg.Prefix
	}
	if cfg.NegativeTTL < 0 || cfg.LoadTimeout < 0 || cfg.RedisTimeout < 0 {
		return nil, errors.New("cache: NegativeTTL, LoadTimeout and RedisTimeout must be >= 0")
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
	if cfg.RedisTimeout == 0 {
		cfg.RedisTimeout = DefaultRedisTimeout
	}
	if cfg.Codec == nil {
		cfg.Codec = JSON[V]{}
	}
	cfg.Logger = log.OrDefault(cfg.Logger)
	if cfg.MeterProvider == nil {
		cfg.MeterProvider = otel.GetMeterProvider()
	}
	c := &Cache[V]{cfg: cfg, log: cfg.Logger, seed: maphash.MakeSeed()}

	if lc := cfg.Local; lc != nil {
		if err := c.newL1(lc); err != nil {
			return nil, err
		}
	} else if cfg.Weigher != nil {
		return nil, errors.New("cache: Weigher requires Local.MaxWeight")
	}

	m, err := newInstruments(cfg.MeterProvider, cfg.Name)
	if err != nil {
		return nil, err
	}
	c.m = m
	c.base, c.stop = context.WithCancel(context.Background()) //nolint:gosec // G118: c.stop is called by Close
	return c, nil
}

func (c *Cache[V]) newL1(lc *LocalConfig) error {
	if lc.TTL <= 0 {
		return errors.New("cache: Local.TTL must be > 0")
	}
	if (lc.MaxSize > 0) == (lc.MaxWeight > 0) || lc.MaxSize < 0 {
		return errors.New("cache: exactly one of Local.MaxSize and Local.MaxWeight must be > 0")
	}
	if (lc.MaxWeight > 0) != (c.cfg.Weigher != nil) {
		return errors.New("cache: Weigher is required with Local.MaxWeight, and only with it")
	}
	if lc.RefreshAfter < 0 || lc.RefreshAfter > 0 && lc.RefreshAfter >= lc.TTL {
		return errors.New("cache: Local.RefreshAfter must be in [0, Local.TTL)")
	}
	opts := &otter.Options[string, entry[V]]{
		MaximumSize: lc.MaxSize,
		ExpiryCalculator: otter.ExpiryWritingFunc(func(e otter.Entry[string, entry[V]]) time.Duration {
			ttl := lc.TTL
			if e.Value.negative && c.cfg.NegativeTTL > 0 {
				ttl = min(ttl, c.cfg.NegativeTTL)
			}
			if e.Value.ttl > 0 {
				ttl = min(ttl, e.Value.ttl)
			}
			return c.jitterDown(ttl)
		}),
		Executor: c.goTracked,
		Logger:   otterLogger{log: c.cfg.Logger, name: c.cfg.Name},
	}
	if lc.MaxWeight > 0 {
		weigh := c.cfg.Weigher
		opts.MaximumWeight = lc.MaxWeight
		opts.Weigher = func(k string, e entry[V]) uint32 {
			if e.negative {
				return 1
			}
			return max(weigh(k, e.v), 1)
		}
	}
	if lc.RefreshAfter > 0 {
		opts.RefreshCalculator = otter.RefreshWriting[string, entry[V]](lc.RefreshAfter)
		c.refresh = true
	}
	l1, err := otter.New(opts)
	if err != nil {
		return fmt.Errorf("cache: local tier: %w", err)
	}
	c.l1 = l1
	return nil
}

// Close cancels the loads still running (their loaders see ctx cancelled),
// waits for them and for background refreshes until ctx is done, and stops
// the L1 background goroutines. It does not close the Redis client. After
// Close the loading methods return ErrClosed. Close is idempotent.
func (c *Cache[V]) Close(ctx context.Context) error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.stop()
	err := c.track.close(ctx)
	if c.l1 != nil {
		c.l1.StopAllGoroutines()
	}
	return err
}

// goTracked is otter's executor: refreshes run on goroutines Close waits for.
func (c *Cache[V]) goTracked(fn func()) {
	if !c.track.add() {
		fn() // closed: run inline rather than leak a goroutine
		return
	}
	go func() {
		defer c.track.done()
		fn()
	}()
}

// detach returns a context with ctx's values that is not cancelled by ctx,
// only by Close.
func (c *Cache[V]) detach(ctx context.Context) (context.Context, context.CancelFunc) {
	d, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(c.base, cancel)
	return d, func() { stop(); cancel() }
}

func (c *Cache[V]) redisKey(key string) string { return c.cfg.Prefix + key }

func (c *Cache[V]) clone(v V) V {
	if c.cfg.Clone == nil {
		return v
	}
	return c.cfg.Clone(v)
}

// jitterDown shortens d by up to Jitter, uniformly.
func (c *Cache[V]) jitterDown(d time.Duration) time.Duration {
	if c.cfg.Jitter <= 0 {
		return d
	}
	return max(d-(timex.Jitter(d, c.cfg.Jitter)-d).Abs(), 1)
}

// l2TTL is the Redis expiry of e, jittered.
func (c *Cache[V]) l2TTL(e entry[V]) time.Duration {
	ttl := c.cfg.TTL
	switch {
	case e.negative:
		ttl = c.cfg.NegativeTTL
	case e.ttl > 0:
		ttl = e.ttl
	}
	return timex.Jitter(ttl, c.cfg.Jitter)
}

func millis(d time.Duration) int64 { return max(d.Milliseconds(), 1) }

// --- L1 write guard ---

func (c *Cache[V]) stripe(key string) *atomic.Uint64 {
	return &c.gens[maphash.String(c.seed, key)%genStripes]
}

// gen is the key's write generation: it changes whenever a local write may
// have touched the key.
func (c *Cache[V]) gen(key string) uint64 { return c.stripe(key).Load() + c.epoch.Load() }

// bump must be called before a local write to the key's L1 entry.
func (c *Cache[V]) bump(key string) { c.stripe(key).Add(1) }

// fill puts e in L1 if the key is absent and no local write happened since
// snap was read. It reports whether no write happened. A write that races
// with the fill bumps the generation first, so the fill sees it and undoes
// itself: the worst case is a miss, never an old value.
func (c *Cache[V]) fill(key string, e entry[V], snap uint64) bool {
	if c.gen(key) != snap {
		return false
	}
	if c.l1 == nil {
		return true
	}
	c.l1.SetIfAbsent(key, e)
	if c.gen(key) != snap {
		c.l1.Invalidate(key)
		return false
	}
	return true
}

// --- codec ---

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

// decode parses a Redis payload; lease reports a lease (a miss).
func (c *Cache[V]) decode(b []byte) (e entry[V], lease bool, err error) {
	if len(b) == 0 {
		return e, false, errors.New("cache: decode: empty payload")
	}
	switch b[0] {
	case tagNegative:
		e.negative = true
		return e, false, nil
	case tagLease:
		return e, true, nil
	case tagValue:
		if err := c.cfg.Codec.Unmarshal(b[1:], &e.v); err != nil {
			return e, false, fmt.Errorf("cache: decode: %w", err)
		}
		return e, false, nil
	default:
		return e, false, fmt.Errorf("cache: decode: unknown tag %q", b[0])
	}
}

// keyAttr identifies a key in logs without writing it: keys may hold
// personal data or secrets.
func keyAttr(key string) log.Attr {
	sum := sha256.Sum256([]byte(key))
	return log.String("cache.key_hash", hex.EncodeToString(sum[:8]))
}

// --- plain reads and writes ---

// readL2 reads key from Redis (GET + PTTL in one round trip). found=false
// with err=nil is a miss.
func (c *Cache[V]) readL2(ctx context.Context, key string) (e entry[V], found bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.RedisTimeout)
	defer cancel()
	var get *redis.StringCmd
	var ttl *redis.DurationCmd
	_, err = c.cfg.Redis.Pipelined(ctx, func(p redis.Pipeliner) error {
		get = p.Get(ctx, c.redisKey(key))
		ttl = p.PTTL(ctx, c.redisKey(key))
		return nil
	})
	return c.parseRead(ctx, get, ttl, err)
}

func (c *Cache[V]) parseRead(ctx context.Context, get *redis.StringCmd, ttl *redis.DurationCmd, pipeErr error) (e entry[V], found bool, err error) {
	b, err := get.Bytes()
	if err == nil {
		err = ttl.Err()
	}
	if err == nil && pipeErr != nil && !errors.Is(pipeErr, redis.Nil) {
		err = pipeErr
	}
	switch {
	case errors.Is(err, redis.Nil):
		c.brk.observe(nil)
		c.count(ctx, c.m.l2Miss)
		return e, false, nil
	case err != nil:
		c.brk.observe(err)
		c.count(ctx, c.m.l2Error)
		return e, false, fmt.Errorf("cache: redis get: %w", err)
	}
	c.brk.observe(nil)
	e, lease, err := c.decode(b)
	switch {
	case err != nil:
		c.count(ctx, c.m.l2Error)
		return e, false, err
	case lease:
		c.count(ctx, c.m.l2Miss)
		return e, false, nil
	}
	if d := ttl.Val(); d > 0 {
		e.ttl = d
	}
	c.count(ctx, c.m.l2Hit)
	return e, true, nil
}

// Get returns the cached value for key. A miss, or a cached negative answer,
// returns ErrNotFound. Redis errors are returned (Get does not fail open).
func (c *Cache[V]) Get(ctx context.Context, key string) (V, error) {
	var zero V
	if c.l1 != nil {
		if e, ok := c.l1.GetIfPresent(key); ok {
			c.count(ctx, c.m.l1Hit)
			return c.result(e)
		}
		c.count(ctx, c.m.l1Miss)
	}
	if c.cfg.Redis == nil {
		return zero, ErrNotFound
	}
	snap := c.gen(key)
	e, found, err := c.readL2(ctx, key)
	if err != nil {
		return zero, err
	}
	if !found {
		return zero, ErrNotFound
	}
	c.fill(key, e, snap)
	return c.result(e)
}

func (c *Cache[V]) result(e entry[V]) (V, error) {
	if e.negative {
		var zero V
		return zero, ErrNotFound
	}
	return c.clone(e.v), nil
}

// SetOption configures Set.
type SetOption func(*setOpts)

type setOpts struct{ ttl time.Duration }

// WithTTL overrides the TTL of one entry: Redis uses it instead of
// Config.TTL, L1 keeps the entry for at most min(TTL, Local.TTL), on every
// pod that reads it.
func WithTTL(d time.Duration) SetOption { return func(o *setOpts) { o.ttl = d } }

// Set stores v under key in every configured tier. L1 is updated even when
// the Redis write fails; the Redis error is returned. A load of key running
// meanwhile does not overwrite v.
func (c *Cache[V]) Set(ctx context.Context, key string, v V, opts ...SetOption) error {
	var o setOpts
	for _, opt := range opts {
		opt(&o)
	}
	if o.ttl < 0 {
		return errors.New("cache: negative TTL")
	}
	e := entry[V]{v: c.clone(v), ttl: o.ttl}
	if c.l1 != nil {
		c.bump(key)
		c.l1.Set(key, e)
	}
	if c.cfg.Redis == nil {
		return nil
	}
	b, err := c.encode(e)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.RedisTimeout)
	defer cancel()
	err = c.cfg.Redis.Set(ctx, c.redisKey(key), b, c.l2TTL(e)).Err()
	c.brk.observe(err)
	if err != nil {
		return fmt.Errorf("cache: redis set: %w", err)
	}
	return nil
}

// Delete removes keys from L1 (this pod only) and Redis. A load of one of
// the keys running meanwhile, on any pod, does not write its result back.
func (c *Cache[V]) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if c.l1 != nil {
		for _, k := range keys {
			c.bump(k)
			c.l1.Invalidate(k)
		}
	}
	if c.cfg.Redis == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.RedisTimeout)
	defer cancel()
	// One DEL per key keeps Redis Cluster happy (no CROSSSLOT).
	_, err := c.cfg.Redis.Pipelined(ctx, func(p redis.Pipeliner) error {
		for _, k := range keys {
			p.Del(ctx, c.redisKey(k))
		}
		return nil
	})
	c.brk.observe(err)
	if err != nil {
		return fmt.Errorf("cache: redis del: %w", err)
	}
	return nil
}

// --- read-through ---

// PanicError is returned when a loader (or a Codec) panics; the panic does
// not crash the process.
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

// acquireL2 reads key from Redis for a load. On a miss it takes a lease.
// observed is what the key held (the lease, or a payload that did not
// decode); a write-back must find it unchanged. observed is empty when Redis
// is off, skipped or failed: then the load does not write Redis.
func (c *Cache[V]) acquireL2(ctx context.Context, key string) (e entry[V], found bool, observed string) {
	if c.cfg.Redis == nil {
		return e, false, ""
	}
	if !c.brk.allow() {
		c.count(ctx, c.m.l2Skipped)
		return e, false, ""
	}
	tctx, cancel := context.WithTimeout(ctx, c.cfg.RedisTimeout)
	defer cancel()
	res, err := acquireScript.Run(tctx, c.cfg.Redis, []string{c.redisKey(key)}, c.newLease(), c.leaseMillis()).Slice()
	c.brk.observe(err)
	if err != nil {
		c.count(ctx, c.m.l2Error)
		c.warn(ctx, &c.warnRead, msgLookup, keyAttr(key), log.Err(err))
		return e, false, ""
	}
	return c.parseAcquired(ctx, key, res)
}

func (c *Cache[V]) parseAcquired(ctx context.Context, key string, res []any) (e entry[V], found bool, observed string) {
	if len(res) != 2 {
		c.count(ctx, c.m.l2Error)
		return e, false, ""
	}
	raw, _ := res[0].(string)
	pttl, _ := res[1].(int64)
	e, lease, err := c.decode([]byte(raw))
	switch {
	case err != nil:
		// Unreadable (another codec or version): load, and replace it.
		c.count(ctx, c.m.l2Error)
		c.warn(ctx, &c.warnRead, msgUnreadable, keyAttr(key), log.Err(err))
		return e, false, raw
	case lease:
		c.count(ctx, c.m.l2Miss)
		return e, false, raw
	}
	if pttl > 0 {
		e.ttl = time.Duration(pttl) * time.Millisecond
	}
	c.count(ctx, c.m.l2Hit)
	return e, true, ""
}

func (c *Cache[V]) newLease() string { return string(tagLease) + rand.Text() }

func (c *Cache[V]) leaseMillis() int64 { return millis(c.cfg.LoadTimeout + c.cfg.RedisTimeout) }

// storeArgs returns the store script arguments for e, or ok=false (logged)
// if e does not encode.
func (c *Cache[V]) storeArgs(ctx context.Context, key, observed string, e entry[V]) ([]any, bool) {
	b, err := c.encode(e)
	if err != nil {
		c.warn(ctx, &c.warnWrite, msgStore, keyAttr(key), log.Err(err))
		return nil, false
	}
	return []any{observed, b, millis(c.l2TTL(e))}, true
}

// storeL2 writes a loaded entry back if the key still holds observed. It
// reports false only when the key changed meanwhile (the entry is stale);
// a Redis error is logged and reported as true (fail open).
func (c *Cache[V]) storeL2(ctx context.Context, key, observed string, e entry[V]) bool {
	args, ok := c.storeArgs(ctx, key, observed, e)
	if !ok {
		return true
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.RedisTimeout)
	defer cancel()
	n, err := storeScript.Run(ctx, c.cfg.Redis, []string{c.redisKey(key)}, args...).Int()
	c.brk.observe(err)
	if err != nil {
		c.warn(ctx, &c.warnWrite, msgStore, keyAttr(key), log.Err(err))
		return true
	}
	return n == 1
}

// load calls the loader and maps its result to an entry: a not-found answer
// becomes a negative entry when NegativeTTL > 0.
func (c *Cache[V]) load(ctx context.Context, key string, load func(ctx context.Context, key string) (V, error)) (entry[V], error) {
	start := time.Now()
	v, err := callLoad(ctx, c.cfg.LoadTimeout, func(ctx context.Context) (V, error) { return load(ctx, key) })
	c.recordLoad(ctx, start, err)
	switch {
	case err == nil:
		return entry[V]{v: v}, nil
	case errors.Is(err, ErrNotFound) && c.cfg.NegativeTTL > 0:
		return entry[V]{negative: true}, nil
	}
	return entry[V]{}, err
}

type flightResult[V any] struct {
	e entry[V]
	// cacheable is false when a write to the key (Set or Delete, on any
	// pod) ran during the load: e may be older than that write.
	cacheable bool
}

// flight loads key once per process: L2 first, then load. It runs detached
// from the caller (bounded by LoadTimeout and cancelled by Close); the
// caller waits until its own ctx is done. A value loaded from the source is
// written to Redis (if the key is unchanged) and to L1 (if no local write
// ran meanwhile), so it is kept even when every caller gave up.
func (c *Cache[V]) flight(ctx context.Context, key string, load func(ctx context.Context, key string) (V, error)) (flightResult[V], error) {
	ch := c.sf.DoChan(key, func() (any, error) { return c.runFlight(ctx, key, load) })
	select {
	case <-ctx.Done():
		return flightResult[V]{}, ctx.Err()
	case r := <-ch:
		if r.Err != nil {
			return flightResult[V]{}, r.Err
		}
		return r.Val.(flightResult[V]), nil
	}
}

func (c *Cache[V]) runFlight(ctx context.Context, key string, load func(ctx context.Context, key string) (V, error)) (res flightResult[V], err error) {
	if !c.track.add() {
		return res, ErrClosed
	}
	defer c.track.done()
	lctx, cancel := c.detach(ctx)
	defer cancel()
	// singleflight re-panics in a new goroutine, which would crash the
	// process: a panicking Codec becomes an error too.
	defer func() {
		if p := recover(); p != nil {
			res, err = flightResult[V]{}, &PanicError{Value: p}
		}
	}()

	snap := c.gen(key)
	e, found, observed := c.acquireL2(lctx, key)
	if found {
		return flightResult[V]{e: e, cacheable: c.fill(key, e, snap)}, nil
	}
	e, err = c.load(lctx, key, load)
	if err != nil {
		return res, err
	}
	fresh := observed == "" || c.storeL2(lctx, key, observed, e)
	return flightResult[V]{e: e, cacheable: fresh && c.fill(key, e, snap)}, nil
}

// uncached carries a loaded value that must be returned but not kept in L1.
type uncached[V any] struct{ e entry[V] }

func (*uncached[V]) Error() string { return "cache: value not cacheable" }

// l1Loader adapts flight to otter. Load runs for the caller that missed;
// Reload runs in the background for a refresh.
type l1Loader[V any] struct {
	c    *Cache[V]
	load func(ctx context.Context, key string) (V, error)
}

func (l *l1Loader[V]) Load(ctx context.Context, key string) (entry[V], error) {
	r, err := l.c.flight(ctx, key, l.load)
	switch {
	case err != nil:
		return entry[V]{}, err
	case !r.cacheable:
		return entry[V]{}, &uncached[V]{e: r.e}
	}
	return r.e, nil
}

func (l *l1Loader[V]) Reload(ctx context.Context, key string, _ entry[V]) (entry[V], error) {
	// ctx is the request that found the entry stale; it usually ends right
	// after. The refresh must not end with it.
	r, err := l.c.flight(context.WithoutCancel(ctx), key, l.load)
	switch {
	case errors.Is(err, ErrNotFound):
		return entry[V]{}, otter.ErrNotFound // gone from the source: drop it
	case err != nil:
		return entry[V]{}, err
	case !r.cacheable:
		return entry[V]{}, otter.ErrNotFound // changed meanwhile: drop it
	}
	return r.e, nil
}

// GetOrLoad returns the cached value for key or calls load, caches its result
// and returns it. Concurrent callers for the same key in this process share
// one load call, whatever load function each passed: the first caller's
// load (and its ctx values) serve them all.
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
	if c.l1 == nil {
		r, err := c.flight(ctx, key, load)
		if err != nil {
			return zero, err
		}
		return c.result(r.e)
	}
	if !c.refresh {
		if e, ok := c.l1.GetIfPresent(key); ok {
			c.count(ctx, c.m.l1Hit)
			return c.result(e)
		}
	} else if ent, ok := c.l1.GetEntry(key); ok {
		c.count(ctx, c.m.l1Hit)
		if !time.Now().Before(ent.RefreshableAt()) {
			// Get starts the background refresh and returns at once.
			_, _ = c.l1.Get(ctx, key, &l1Loader[V]{c: c, load: load})
		}
		return c.result(ent.Value)
	}
	c.count(ctx, c.m.l1Miss)
	e, err := c.l1.Get(ctx, key, &l1Loader[V]{c: c, load: load})
	if u, ok := errors.AsType[*uncached[V]](err); ok {
		e, err = u.e, nil
	}
	if err != nil {
		return zero, err
	}
	return c.result(e)
}

// GetMany returns the cached values among keys; missing and negative entries
// are absent from the map. Redis lookups are pipelined. Redis errors and
// unreadable entries are returned (joined) along with the values that were
// read.
func (c *Cache[V]) GetMany(ctx context.Context, keys []string) (map[string]V, error) {
	found := make(map[string]entry[V], len(keys))
	var rest []string
	for _, k := range dedupe(keys) {
		if c.l1 != nil {
			if e, ok := c.l1.GetIfPresent(k); ok {
				c.count(ctx, c.m.l1Hit)
				found[k] = e
				continue
			}
			c.count(ctx, c.m.l1Miss)
		}
		rest = append(rest, k)
	}
	var errList []error
	if c.cfg.Redis != nil && len(rest) > 0 {
		snaps := make([]uint64, len(rest))
		for i, k := range rest {
			snaps[i] = c.gen(k)
		}
		gets := make([]*redis.StringCmd, len(rest))
		ttls := make([]*redis.DurationCmd, len(rest))
		tctx, cancel := context.WithTimeout(ctx, c.cfg.RedisTimeout)
		_, pipeErr := c.cfg.Redis.Pipelined(tctx, func(p redis.Pipeliner) error {
			for i, k := range rest {
				gets[i] = p.Get(tctx, c.redisKey(k))
				ttls[i] = p.PTTL(tctx, c.redisKey(k))
			}
			return nil
		})
		cancel()
		for i, k := range rest {
			e, ok, err := c.parseRead(ctx, gets[i], ttls[i], pipeErr)
			switch {
			case err != nil:
				errList = append(errList, err)
			case ok:
				c.fill(k, e, snaps[i])
				found[k] = e
			}
		}
	}
	out := make(map[string]V, len(found))
	for k, e := range found {
		if !e.negative {
			out[k] = c.clone(e.v)
		}
	}
	return out, errors.Join(errList...)
}

func dedupe(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	out := keys[:0:0]
	for _, k := range keys {
		if _, dup := seen[k]; !dup {
			seen[k] = struct{}{}
			out = append(out, k)
		}
	}
	return out
}

// GetOrLoadMany returns values for keys, calling load once with the keys that
// are not cached. Keys that load omits are treated as not found (and cached
// negatively when NegativeTTL > 0). With L1, a key that another call is
// already loading is waited for instead of loaded again (otter BulkGet);
// without L1, concurrent calls are not merged. Redis failures fail open, as
// in GetOrLoad; load runs with the caller's ctx bounded by LoadTimeout and a
// panic is returned as a *PanicError.
func (c *Cache[V]) GetOrLoadMany(ctx context.Context, keys []string, load func(ctx context.Context, missing []string) (map[string]V, error)) (map[string]V, error) {
	var found map[string]entry[V]
	var err error
	if c.l1 != nil {
		keys = dedupe(keys)
		var missed atomic.Int64
		found, err = c.l1.BulkGet(ctx, keys, otter.BulkLoaderFunc[string, entry[V]](func(ctx context.Context, keys []string) (map[string]entry[V], error) {
			missed.Add(int64(len(keys)))
			return c.loadMany(ctx, keys, load)
		}))
		m := missed.Load()
		c.m.requests.Add(ctx, int64(len(keys))-m, c.m.l1Hit.add...)
		c.m.requests.Add(ctx, m, c.m.l1Miss.add...)
	} else {
		found, err = c.loadMany(ctx, dedupe(keys), load)
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

// loadMany reads keys from L2 (taking leases on misses) and loads the rest
// from the source, writing results back to L2 where the key is unchanged
// (otter commits them to L1). Keys changed meanwhile are returned with a
// TTL that keeps them out of L1.
func (c *Cache[V]) loadMany(ctx context.Context, keys []string, load func(ctx context.Context, missing []string) (map[string]V, error)) (found map[string]entry[V], err error) {
	defer func() {
		if p := recover(); p != nil {
			found, err = nil, &PanicError{Value: p}
		}
	}()
	found = make(map[string]entry[V], len(keys))
	observed := make(map[string]string, len(keys))
	var missing []string
	switch {
	case c.cfg.Redis == nil || len(keys) == 0:
		missing = keys
	case !c.brk.allow():
		c.m.requests.Add(ctx, int64(len(keys)), c.m.l2Skipped.add...)
		missing = keys
	default:
		lease, ms := c.newLease(), c.leaseMillis()
		cmds := c.runScripts(ctx, acquireScript, keys, func(string) []any { return []any{lease, ms} })
		for i, k := range keys {
			res, err := cmds[i].Slice()
			if err != nil {
				c.count(ctx, c.m.l2Error)
				c.warn(ctx, &c.warnRead, msgBatchLookup, keyAttr(k), log.Err(err))
				missing = append(missing, k)
				continue
			}
			e, ok, obs := c.parseAcquired(ctx, k, res)
			if ok {
				found[k] = e
				continue
			}
			if obs != "" {
				observed[k] = obs
			}
			missing = append(missing, k)
		}
	}
	if len(missing) == 0 {
		return found, nil
	}

	start := time.Now()
	loaded, err := callLoad(ctx, c.cfg.LoadTimeout, func(ctx context.Context) (map[string]V, error) { return load(ctx, missing) })
	c.recordLoad(ctx, start, err)
	if err != nil {
		return nil, err
	}
	var toStore []string
	args := make(map[string][]any, len(missing))
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
		if obs, ok := observed[k]; ok {
			if a, ok := c.storeArgs(ctx, k, obs, e); ok {
				toStore = append(toStore, k)
				args[k] = a
			}
		}
	}
	if len(toStore) > 0 {
		cmds := c.runScripts(ctx, storeScript, toStore, func(k string) []any { return args[k] })
		for i, k := range toStore {
			n, err := cmds[i].Int()
			if err != nil {
				c.warn(ctx, &c.warnWrite, msgStore, keyAttr(k), log.Err(err))
				continue
			}
			if n == 0 { // changed meanwhile: return it, do not keep it
				e := found[k]
				e.ttl = noCacheTTL
				found[k] = e
			}
		}
	}
	return found, nil
}

// runScripts runs s once per key in one pipeline (EVALSHA, then EVAL for the
// keys Redis reports NOSCRIPT for) and returns one command per key.
func (c *Cache[V]) runScripts(ctx context.Context, s *redis.Script, keys []string, args func(key string) []any) []*redis.Cmd {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.RedisTimeout)
	defer cancel()
	cmds := make([]*redis.Cmd, len(keys))
	_, err := c.cfg.Redis.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, k := range keys {
			cmds[i] = s.EvalSha(ctx, p, []string{c.redisKey(k)}, args(k)...)
		}
		return nil
	})
	if err != nil && redis.HasErrorPrefix(err, "NOSCRIPT") {
		_, err = c.cfg.Redis.Pipelined(ctx, func(p redis.Pipeliner) error {
			for i, k := range keys {
				if redis.HasErrorPrefix(cmds[i].Err(), "NOSCRIPT") {
					cmds[i] = s.Eval(ctx, p, []string{c.redisKey(k)}, args(k)...)
				}
			}
			return nil
		})
	}
	c.brk.observe(err)
	return cmds
}

// DeleteByPrefix removes every key starting with prefix (relative to
// Config.Prefix) from L1 (this pod) and Redis, using SCAN + UNLINK on every
// shard ([redisx.ForEachShard]). It returns the number of Redis keys
// removed. It is O(keyspace); use it for admin and invalidation jobs, not on
// hot paths. It is bounded by ctx, not by RedisTimeout.
func (c *Cache[V]) DeleteByPrefix(ctx context.Context, prefix string) (int, error) {
	if c.l1 != nil {
		c.epoch.Add(1)
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
			if len(keys) > 0 {
				// One UNLINK per key, pipelined: keys of one SCAN batch may
				// hash to different slots in Cluster mode.
				cmds, err := cl.Pipelined(ctx, func(p redis.Pipeliner) error {
					for _, k := range keys {
						p.Unlink(ctx, k)
					}
					return nil
				})
				for _, cmd := range cmds {
					if ic, ok := cmd.(*redis.IntCmd); ok {
						total += int(ic.Val())
					}
				}
				if err != nil {
					return err
				}
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

// --- metrics ---

type instruments struct {
	requests metric.Int64Counter
	loadDur  metric.Float64Histogram

	l1Hit, l1Miss, l2Hit, l2Miss, l2Error, l2Skipped attrs
	loadOK, loadErr, loadNotFound                    attrs
}

// attrs holds one precomputed attribute set as ready-made option slices, so
// recording allocates nothing.
type attrs struct {
	add []metric.AddOption
	rec []metric.RecordOption
}

func newInstruments(mp metric.MeterProvider, name string) (instruments, error) {
	meter := mp.Meter(scopeName)
	var m instruments
	var err error
	m.requests, err = meter.Int64Counter("kitchen.cache.requests",
		metric.WithDescription("Cache lookups by tier and result."),
		metric.WithUnit("{request}"))
	if err != nil {
		return m, fmt.Errorf("cache: metric: %w", err)
	}
	m.loadDur, err = meter.Float64Histogram("kitchen.cache.load.duration",
		metric.WithDescription("Duration of loader calls."),
		metric.WithUnit("s"))
	if err != nil {
		return m, fmt.Errorf("cache: metric: %w", err)
	}
	// Attribute sets are built once: recording costs no allocation.
	opt := func(tier, result string) attrs {
		o := metric.WithAttributeSet(attribute.NewSet(attribute.String("cache.name", name),
			attribute.String("cache.tier", tier), attribute.String("cache.result", result)))
		return attrs{add: []metric.AddOption{o}, rec: []metric.RecordOption{o}}
	}
	m.l1Hit, m.l1Miss = opt("l1", "hit"), opt("l1", "miss")
	m.l2Hit, m.l2Miss, m.l2Error, m.l2Skipped = opt("l2", "hit"), opt("l2", "miss"), opt("l2", "error"), opt("l2", "skipped")
	m.loadOK, m.loadErr, m.loadNotFound = opt("loader", "ok"), opt("loader", "error"), opt("loader", "not_found")
	return m, nil
}

func (c *Cache[V]) count(ctx context.Context, a attrs) {
	c.m.requests.Add(ctx, 1, a.add...)
}

func (c *Cache[V]) recordLoad(ctx context.Context, start time.Time, err error) {
	o := c.m.loadOK
	switch {
	case errors.Is(err, ErrNotFound):
		o = c.m.loadNotFound
	case err != nil:
		o = c.m.loadErr
	}
	c.m.requests.Add(ctx, 1, o.add...)
	c.m.loadDur.Record(ctx, time.Since(start).Seconds(), o.rec...)
}

// --- helpers: breaker, throttle, tracker, otter logger ---

// breaker skips Redis on the read-through paths after breakerThreshold
// consecutive failures, for breakerCooldown. The first call after the
// cooldown probes Redis again; one more failure reopens it.
type breaker struct {
	failures  atomic.Int32
	openUntil atomic.Int64
}

func (b *breaker) allow() bool { return time.Now().UnixNano() >= b.openUntil.Load() }

func (b *breaker) observe(err error) {
	switch {
	case err == nil || errors.Is(err, redis.Nil):
		b.failures.Store(0)
	case errors.Is(err, context.Canceled):
		// the caller gave up: says nothing about Redis
	case b.failures.Add(1) >= breakerThreshold:
		b.openUntil.Store(time.Now().Add(breakerCooldown).UnixNano())
	}
}

// throttle lets one warning of a kind through per warnInterval and counts
// the others.
type throttle struct {
	next    atomic.Int64
	dropped atomic.Int64
}

type warnMsg int

const (
	msgLookup warnMsg = iota
	msgBatchLookup
	msgUnreadable
	msgStore
)

func (c *Cache[V]) warn(ctx context.Context, t *throttle, m warnMsg, attrs ...log.Attr) {
	now := time.Now().UnixNano()
	n := t.next.Load()
	if now < n || !t.next.CompareAndSwap(n, now+int64(warnInterval)) {
		t.dropped.Add(1)
		return
	}
	attrs = append(attrs, log.String("cache", c.cfg.Name))
	if d := t.dropped.Swap(0); d > 0 {
		attrs = append(attrs, log.Int64("suppressed", d))
	}
	switch m {
	case msgLookup:
		c.log.LogAttrs(ctx, log.LevelWarn, "cache: lookup failed, loading from source", attrs...)
	case msgBatchLookup:
		c.log.LogAttrs(ctx, log.LevelWarn, "cache: batch lookup failed, loading from source", attrs...)
	case msgUnreadable:
		c.log.LogAttrs(ctx, log.LevelWarn, "cache: unreadable entry, loading from source", attrs...)
	case msgStore:
		c.log.LogAttrs(ctx, log.LevelWarn, "cache: store failed", attrs...)
	}
}

// tracker counts goroutines Close waits for. add fails once closed, so the
// WaitGroup is never added to while Close waits.
type tracker struct {
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func (t *tracker) add() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	t.wg.Add(1)
	return true
}

func (t *tracker) done() { t.wg.Done() }

func (t *tracker) close(ctx context.Context) error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	done := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("cache: close: %w", ctx.Err())
	}
}

// otterLogger routes otter's own warnings (failed refreshes) to the cache's
// logger.
type otterLogger struct {
	log  *log.Logger
	name string
}

func (l otterLogger) Warn(ctx context.Context, msg string, err error) {
	l.log.WarnContext(ctx, "cache: local tier warning", log.String("cache", l.name), log.String("detail", msg), log.Err(err))
}

func (l otterLogger) Error(ctx context.Context, msg string, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, ErrClosed) {
		return // shutdown
	}
	l.log.ErrorContext(ctx, "cache: refresh failed", log.String("cache", l.name), log.String("detail", msg), log.Err(err))
}
