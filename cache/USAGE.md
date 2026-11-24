# cache

`cache` is a typed, two-tier, read-through cache. L1 is an in-process cache
(otter). L2 is Redis. Either tier can be turned off. Use it whenever a
service caches values. Do not use otter, ristretto or go-redis `GET`/`SET`
by hand for this. It is the canonical cache package
([D7](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22)).

## Install

```sh
go get github.com/dungnh3/kitchen
```

`cache` and `redisx` are both in the core module.

## Quick start

Open Redis with `redisx.Open`, then build a cache for one value type:

```go
ctx := context.Background()
mr, _ := miniredis.Run() // in a service: your Redis address
defer mr.Close()

rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
if err != nil {
	panic(err)
}
defer func() { _ = rdb.Close() }()

products, err := cache.New(cache.Config[product]{
	Redis:  rdb,
	Prefix: "product:v1:",
	TTL:    10 * time.Minute,
	Local:  &cache.LocalConfig{MaxSize: 10_000, TTL: 30 * time.Second},
	Logger: log.Default(), // in a service: the *log.Logger from main
})
if err != nil {
	panic(err)
}
defer func() { _ = products.Close(ctx) }()

p, err := products.GetOrLoad(ctx, "p1", func(_ context.Context, id string) (product, error) {
	return product{ID: id, Price: 100}, nil // e.g. repo.GetProduct(ctx, id)
})
fmt.Println(p.Price, err)

_ = products.Set(ctx, "p2", product{ID: "p2", Price: 250})
p, err = products.Get(ctx, "p2")
fmt.Println(p.Price, err)

_ = products.Delete(ctx, "p2")
_, err = products.Get(ctx, "p2")
fmt.Println(errors.Is(err, cache.ErrNotFound))
```

What the defaults give you:

- `GetOrLoad` checks L1, then Redis, then calls the loader. Concurrent
  callers for one key share a single loader call (singleflight), so a cold
  key costs one load per pod, not one per request.
- Redis failures in `GetOrLoad` fail open: they are logged (at most once
  per 10s) and counted, and the loader answers. Each Redis command is
  bounded by 500ms, and after 5 failures in a row Redis is skipped for 5s.
- A `Set` or `Delete` that runs while a key is loading wins: the load
  cannot write its older value back, on this pod or in Redis.
- Redis TTLs get ±10 % jitter and L1 TTLs up to −10 %, so keys written
  together do not expire together.
- A loader is bounded by a 30s timeout, and a panic in it becomes an
  error instead of a crash.
- Values are JSON in Redis.

`Close(ctx)` cancels loads still running, waits for them (and for
background refreshes) until `ctx` ends, and stops L1's goroutines. It does
not close the Redis client, which you own.

## Configuration

`cache.Config[V]` fields. At least one of `Redis` and `Local` is required.

| Field | Default | When to change |
|---|---|---|
| `Redis` | nil (no L2) | share entries across pods; pass the client from `redisx.Open` |
| `Prefix` | required with Redis | always; version it (`user:v1:`) so a format change starts clean |
| `TTL` | required with Redis | how stale Redis entries may get |
| `Local` | nil (no L1) | hot keys; saves a Redis round trip |
| `Local.MaxSize` | one of MaxSize/MaxWeight | number of entries kept per pod |
| `Local.MaxWeight` + `Weigher` | off | values of very different sizes: bound L1 by bytes |
| `Local.TTL` | required (> 0) | keep it short: L1 on other pods is not invalidated |
| `Local.RefreshAfter` | 0 (off) | refresh hot keys in the background; must be < `Local.TTL` |
| `NegativeTTL` | 0 (off) | cache "not found" answers from the loader |
| `Jitter` | 0.1 (±10 %) | `-1` disables; must be < 1 |
| `LoadTimeout` | 30s (`DefaultLoadTimeout`) | slow or fast loaders |
| `RedisTimeout` | 500ms (`DefaultRedisTimeout`) | each Redis command; raise it for big values |
| `Clone` | nil | pointer, slice or map values |
| `Codec` | `cache.JSON[V]` | a faster or more compact format |
| `Logger` | `log.Default()` | pass the `*log.Logger` from `main` |
| `MeterProvider` | global | tests, or a custom provider |
| `Name` | `Prefix`; required without it | the `cache.name` metric and log attribute |

## Integration with kitchen

### Redis via redisx

Always open the client with `redisx.Open` (TLS, OTel, timeouts, Cluster and
Sentinel). Its clients honour context deadlines, which `RedisTimeout`
relies on. `cache` accepts any `redis.UniversalClient` and never builds
one. One client can serve many caches, each with its own `Prefix`.

### Logger

Pass the process logger: `Config.Logger`. `main` calls `log.Setup` once;
the cache never configures logging. It logs L2 failures at Warn level, at
most once per kind every 10s. Logs never contain keys (they may hold emails
or tokens): they carry `cache.key_hash`, the first 8 bytes of the key's
SHA-256 in hex. Compute it the same way to find a key's lines.

### Errors

`cache.ErrNotFound` is `errs.ErrNotFound`. `Get` returns it on a miss. A
loader that returns any `errs` error with code NotFound (for example your
repository's `ErrProductNotFound`) is a miss too. Returned from a handler,
it becomes 404 / NotFound. Other loader errors pass through unchanged.

### Telemetry

The cache counts lookups in `kitchen.cache.requests` and times loaders in
`kitchen.cache.load.duration`. Redis commands are
traced by `redisx`. See [Observability](#observability).

### Server wiring

Build caches in `main` (or your wiring code) next to the repositories they
front. Pass them to your services. Full service wiring is in
[server/USAGE.md](../server/USAGE.md).

## Advanced

### Negative caching

Set `NegativeTTL` when lookups for missing keys are common (bots, stale
links). The loader's NotFound answer is cached, so the database is not hit
again:

```go
c, _ := cache.New(cache.Config[product]{
	Name:        "products", // required without Prefix (L1 only)
	Local:       &cache.LocalConfig{MaxSize: 1000, TTL: time.Minute},
	NegativeTTL: 30 * time.Second, // remember "not found" for 30s
})
defer func() { _ = c.Close(ctx) }()

calls := 0
load := func(context.Context, string) (product, error) {
	calls++
	return product{}, ErrProductNotFound // any errs NotFound error
}
for range 3 {
	_, err := c.GetOrLoad(ctx, "missing", load)
	fmt.Println(errors.Is(err, cache.ErrNotFound))
}
fmt.Println("loader calls:", calls) // 1
```

Here `ErrProductNotFound` is
`errs.New(codes.NotFound, "PRODUCT_NOT_FOUND", ...)`. Keep `NegativeTTL`
short: a newly created key stays "missing" until it expires, unless you
`Delete` it on create.

### Batches

`GetOrLoadMany` calls the loader once with every key that is not cached.
Keys the loader omits are not found. The missing keys arrive in no
particular order. `GetMany` only reads. Redis lookups
are pipelined.

```go
_ = c.Set(ctx, "p1", product{ID: "p1", Price: 1})
got, err := c.GetOrLoadMany(ctx, []string{"p1", "p2", "p3"}, func(_ context.Context, missing []string) (map[string]product, error) {
	fmt.Println("load", slices.Sorted(slices.Values(missing))) // one call, any order
	out := map[string]product{}
	for _, id := range missing {
		if id != "p3" { // p3 does not exist: omit it
			out[id] = product{ID: id, Price: 2}
		}
	}
	return out, nil
})
fmt.Println(slices.Sorted(maps.Keys(got)), err)
```

### Per-entry TTL and jitter

`WithTTL` overrides `Config.TTL` for one `Set`. L1 keeps the entry for at
most `min(TTL, Local.TTL)`.

```go
c, _ := cache.New(cache.Config[string]{
	Redis:  rdb,
	Prefix: "otp:",
	TTL:    time.Hour,
	Jitter: -1, // exact TTLs; the default ±10 % spreads expiries
})
defer func() { _ = c.Close(ctx) }()

_ = c.Set(ctx, "u42", "123456", cache.WithTTL(5*time.Minute))
```

Jitter uses `timex.Jitter`. Keep it on for ordinary data: without it, keys
filled together (after a deploy or a flush) expire together and stampede
the database. Turn it off only when the TTL itself has meaning, like a
one-time code.

### Refresh-ahead and L1 only

An L1-only cache needs no Redis. `RefreshAfter` reloads an entry in the
background on the first `GetOrLoad` after it is that old. Callers keep the
current value meanwhile, so hot keys never expire under load.

```go
c, err := cache.New(cache.Config[int]{
	Name:        "answers", // required without Prefix (L1 only)
	Local:       &cache.LocalConfig{MaxSize: 1000, TTL: time.Minute, RefreshAfter: 45 * time.Second},
	LoadTimeout: 2 * time.Second,
})
```

### Load timeout, cancellation and panics

The loader runs detached from the caller that started it, bounded by
`LoadTimeout` and cancelled by `Close`. A caller whose `ctx` ends returns at once, but the load
still fills the cache for the others. In `GetOrLoadMany` the loader uses
the caller's `ctx`, bounded by `LoadTimeout`.

A panic in a loader is returned as `*cache.PanicError`:

```go
_, err := c.GetOrLoad(context.Background(), "k", func(context.Context, string) (int, error) {
	panic("bug in loader")
})
var pe *cache.PanicError
fmt.Println(errors.As(err, &pe), pe.Value)
```

### Pointer, slice and map values

L1 hands the same value to every caller. Set `Clone` for pointer, slice or
map values, or never mutate what the cache returns:

```go
type cart struct{ Items []string }
c, _ := cache.New(cache.Config[*cart]{
	Name:  "carts", // required without Prefix (L1 only)
	Local: &cache.LocalConfig{MaxSize: 1000, TTL: time.Minute},
	Clone: func(c *cart) *cart { return &cart{Items: slices.Clone(c.Items)} },
})
```

### Invalidation and consistency

`Delete` removes keys from this pod's L1 and from Redis. Other pods keep
their L1 copy until `Local.TTL`, so keep that TTL short for data that
changes: it is the bound on how stale another pod can be.

Update the source first, then `Set` or `Delete` the key. A load that runs
at the same time cannot undo that:

- in Redis, a load that misses leaves a lease on the key and writes back
  only if the lease is still there. `Set` and `Delete` replace or remove it
  (the lease scheme of Facebook's "Scaling Memcache");
- in L1, a load's result is dropped if this pod wrote the key meanwhile.

The caller of that load still gets the value it loaded; it is just not
cached. `DeleteByPrefix` removes a whole group with SCAN + UNLINK on every
shard (Cluster-aware). It is O(keyspace): use it in admin and
invalidation jobs, not on hot paths.

```go
c, _ := cache.New(cache.Config[int]{Redis: rdb, Prefix: "price:v1:", TTL: time.Hour})
defer func() { _ = c.Close(ctx) }()
for _, k := range []string{"shop1:a", "shop1:b", "shop2:a"} {
	_ = c.Set(ctx, k, 1)
}
n, err := c.DeleteByPrefix(ctx, "shop1:") // admin job, not a hot path
```

### Custom codec

A `Codec[V]` replaces JSON in Redis. It must be safe for concurrent use.

```go
// rawCodec stores strings as raw bytes instead of JSON.
type rawCodec struct{}

func (rawCodec) Marshal(v string) ([]byte, error)       { return []byte(v), nil }
func (rawCodec) Unmarshal(data []byte, v *string) error { *v = string(data); return nil }
```

Then set `Codec: rawCodec{}`. Redis values are a one-byte tag plus the
codec output. The format is internal. Change `Prefix` when you change the
codec or the value type.

### Fail-open behaviour

Only the read-through paths fail open. `GetOrLoad` and `GetOrLoadMany`
log a Redis error, count it, and call the loader. Every Redis command waits
at most `RedisTimeout`. After 5 failures in a row the read-through paths
skip Redis for 5s (`cache.result=skipped`), then try again. `Get`, `Set`
and `Delete` always try Redis, so an invalidation is never skipped. `Get`, `GetMany`, `Set`,
`Delete` and `DeleteByPrefix` return Redis errors to you. `Set` still
updates L1 when Redis fails.

## Testing

- L1-only caches need nothing: set only `Local` and `Name`.
- For L2, run miniredis and open it with `redisx.Open`, as in the quick
  start. `mr.TTL(key)` and `mr.FastForward(d)` check expiry.
- Pass `Logger: log.Discard()` to keep test output clean.
- Assert with `errors.Is(err, cache.ErrNotFound)` and
  `errors.As(err, &pe)` for `*cache.PanicError`.
- Use `testing/synctest` for TTL and timeout tests, not `time.Sleep`.

## Observability

- **Metric** `kitchen.cache.requests` (counter, `{request}`), attributes:
  - `cache.name`: `Config.Name` (default `Prefix`);
  - `cache.tier`: `l1`, `l2` or `loader`;
  - `cache.result`: `hit`, `miss`, `error` or `skipped` (`ok`,
    `not_found` or `error` for `loader`).
- **Metric** `kitchen.cache.load.duration` (histogram, `s`), with
  `cache.name` and the loader's `cache.result`.
- **Logs** (Warn, at most one per kind per 10s with a `suppressed`
  count): "lookup failed, loading from source", "batch lookup failed,
  loading from source", "unreadable entry, loading from source", "store
  failed", with `cache`, `cache.key_hash` and the error. Failed background
  refreshes: "cache: refresh failed" (Error).
- **Spans:** none of its own. Redis commands are traced by `redisx`;
  trace your loader.
- **Errors:** `ErrNotFound` (`errs.ErrNotFound`), `ErrClosed`,
  `*PanicError`, and wrapped Redis errors (without keys).

## Production checklist

- Open Redis with `redisx.Open`. Never `redis.NewClient`.
- Version `Prefix` (`v1:`); bump it when the value type or codec changes.
- Keep `Local.TTL` short (seconds) for data that changes.
- Set `Clone` for pointer, slice and map values.
- Leave jitter on unless the TTL itself has meaning.
- Set `NegativeTTL` when misses are frequent; keep it short.
- Pass the `*log.Logger` from `main`; call `Close(ctx)` on shutdown, after
  the server has drained.
- Give each cache its own `Prefix`, and make no prefix a prefix of
  another's (`u:` and `u:a`), or `DeleteByPrefix` crosses caches.
- Use `MaxWeight` + `Weigher` when values vary a lot in size.
- Concurrent callers of one key share the first caller's loader and ctx
  values: never make the loaded value depend on who asks.
- Alert on `cache.result=error` or `skipped` for `cache.tier=l2`.
- `GetOrLoadMany` merges concurrent batches only with L1; set `Local`
  (even with a short TTL) when batch loads are hot.

## Migrating from the old kitchen

The old `cache` package maps to `cache.New` with a typed `Config`. The
stored format changed, so use a new `Prefix`. See
[data-utils.md](../docs/migration/data-utils.md#cache--cache).
