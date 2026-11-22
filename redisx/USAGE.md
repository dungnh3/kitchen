# redisx

`redisx` opens an instrumented go-redis client from a plain `Config`. Every
Redis client in kitchen and in services comes from `redisx.Open`, never from
`redis.NewClient` and friends. It is the canonical package for Redis clients
([D7](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22)).

## Install

```sh
go get github.com/dungnh3/kitchen
```

`redisx` lives in the core module. The packages that consume its client are
separate modules: `github.com/dungnh3/kitchen/lease/redis`,
`github.com/dungnh3/kitchen/ratelimit/redis`, `github.com/dungnh3/kitchen/sse`
and `github.com/dungnh3/kitchen/pubsub/reply/redis`. `cache` is in the core
module.

## Quick start

```go
mr, _ := miniredis.Run()
defer mr.Close()

rdb, err := redisx.Open(context.Background(), redisx.Config{Addrs: []string{mr.Addr()}})
if err != nil {
	panic(err)
}
defer func() { _ = rdb.Close() }()
fmt.Println(rdb.Ping(context.Background()).Val())
```

In a service, `Addrs` is your Redis address instead of miniredis.

`Open` returns a `redis.UniversalClient`. The caller owns it and must `Close`
it. What `Open` does for you:

- **Picks the mode from `Config`:** `MasterName` set means Sentinel;
  `Cluster: true` or more than one address means Redis Cluster; otherwise a
  single node.
- **Honours context deadlines.** `ContextTimeoutEnabled` is always on, so a
  command stops when its ctx expires. Without it, go-redis waits for its own
  `ReadTimeout` and a caller with a 100ms budget holds a connection for 3s.
- **Traces and meters** every command through redisotel, using the global
  OTel providers.
- **Pings** the server (5s by default). If anything fails, nothing is left
  open.

## Configuration

`Config` is a plain struct. Zero fields keep go-redis defaults unless stated.

| Field | Default | When to change it |
|---|---|---|
| `Addrs` | required | `host:port` of the node, cluster seeds or sentinels. |
| `MasterName` | empty | Set to use Sentinel. |
| `Cluster` | false | Cluster mode with a single seed address. |
| `Username`, `Password` | empty | ACL user or `requirepass`. |
| `SentinelUsername`, `SentinelPassword` | empty | Credentials of the sentinels themselves. |
| `DB` | 0 | Single node and Sentinel only. Cluster has no databases. |
| `TLS` | nil (plain TCP) | Any managed Redis reached over a network you do not own. |
| `PoolSize` | 10 × GOMAXPROCS per node | High-concurrency services. |
| `MinIdleConns` | 0 | Keep warm connections for latency-sensitive paths. |
| `DialTimeout` | 5s | Slow networks. |
| `ReadTimeout` | 3s | Slow commands or large replies. go-redis extends it for blocking reads itself. |
| `WriteTimeout` | `ReadTimeout` | Rarely. |
| `PingTimeout` | 5s | Bounds the check in `Open`. |

Options: `WithTracerProvider`, `WithMeterProvider` (default: the OTel
globals) and `WithoutTracing` (drops per-command spans, keeps metrics).

`redisx` reads no environment variables. Load the config in `main`.

## Integration with kitchen

### Cluster, Sentinel and TLS

```go
ctx := context.Background()
rdb, err := redisx.Open(ctx, redisx.Config{
	Addrs:    []string{"redis-0:6379", "redis-1:6379", "redis-2:6379"},
	Username: "orders",
	Password: os.Getenv("REDIS_PASSWORD"),
	TLS:      &tls.Config{MinVersion: tls.VersionTLS12},
	PoolSize: 50,
})
if err != nil {
	log.Default().Error("redis", log.Err(err))
	return
}
defer func() { _ = rdb.Close() }()
```

```go
ctx := context.Background()
rdb, err := redisx.Open(ctx, redisx.Config{
	MasterName:       "mymaster",
	Addrs:            []string{"sentinel-0:26379", "sentinel-1:26379", "sentinel-2:26379"},
	SentinelPassword: os.Getenv("SENTINEL_PASSWORD"),
	Password:         os.Getenv("REDIS_PASSWORD"),
	DB:               2,
})
```

`Open` returns plain Go errors, not `errs` errors. A Redis error during a
request is a dependency failure; wrap it with
`errs.Wrap(err, errs.ErrDependencyUnavailable)` before returning it to a
caller.

### Telemetry and logging

`otelx/sdk.Setup` in `main` installs the global providers, and `Open` uses
them. Pass `WithTracerProvider` / `WithMeterProvider` only for a non-global
provider. `redisx` does not log; the packages built on it take a
`*log.Logger` of their own.

### cache

`cache.Config.Redis` takes the client as the L2 tier.

```go
rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
if err != nil {
	panic(err)
}
defer func() { _ = rdb.Close() }()

type user struct{ Name string }
users, err := cache.New(cache.Config[user]{
	Redis:  rdb,
	Prefix: "user:v1:",
	TTL:    10 * time.Minute,
})
```

### lease/redis

`leaseredis.New(rdb, opts...)` returns a `lease.Locker` (default key prefix
`lease:`).

```go
ctx := context.Background()
rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{"localhost:6379"}})
if err != nil {
	return
}
defer func() { _ = rdb.Close() }()
locker, _ := leaseredis.New(rdb)

elector, _ := lease.NewElector(locker, "billing-scheduler")
```

### ratelimit/redis

`rlredis.New(rdb)` returns the shared store for `ratelimit`. It refuses a
client without `ContextTimeoutEnabled`, so its 100ms per-call timeout can
protect requests from a hanging Redis. Clients from `Open` always pass.

```go
store, err := rlredis.New(rdb)
if err != nil {
	panic(err)
}
// Normally: ratelimit.New(ratelimit.Config{Store: ratelimit.RemoteStore(store), Instances: 8, ...})
```

For server-side wiring with `WithRateLimit`, see
[server/USAGE.md](../server/USAGE.md).

### sse/redisbus

Writers append through a `Producer`; gateways read through a `Hub`. Both take
the client and a config struct whose zero value uses the defaults.

```go
p, _ := redisbus.NewProducer(rdb, redisbus.ProducerConfig{})
hub, _ := redisbus.NewHub(rdb, redisbus.HubConfig{})
defer hub.Close()
```

### pubsub/reply/redis

`replyredis.New(rdb, opts...)` returns a `reply.Broker` (default key prefix
`reply:`, one-minute TTL).

```go
// redisx.Open: tracing, metrics, context deadlines honored, ping on open.
rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{"localhost:6379"}})
if err != nil {
	return
}
defer func() { _ = rdb.Close() }()
broker, _ := replyredis.New(rdb)
```

Open one client per Redis deployment in `main` and pass the same client to
every package above. Close it last, after the packages that use it.

## Advanced

### Keyspace-wide commands on a cluster

On a Cluster client, `SCAN`, `KEYS`, `FLUSHDB` and `INFO` go to one node
only. `ForEachShard` runs a function on every master (or every Ring shard,
or once on a single node). Calls are serialised. `GlobEscape` makes a key
prefix safe inside a `MATCH` pattern.

```go
// SCAN runs on every master of a Cluster (and once on a single node).
match := redisx.GlobEscape("tenant[1]:") + "*" // "[1]" is literal, not a class
n := 0
err = redisx.ForEachShard(ctx, rdb, func(ctx context.Context, shard redis.UniversalClient) error {
	it := shard.Scan(ctx, 0, match, 100).Iterator()
	for it.Next(ctx) {
		n++ // calls are serialised: no lock needed
	}
	return it.Err()
})
```

`IsCluster(rdb)` tells you whether multi-key commands (`MGET`, Lua scripts,
`MULTI`) must keep their keys in one hash slot. Use a `{hash tag}` in the key
when it returns true.

### Checking a client you did not open

A package that accepts a `redis.UniversalClient` can check it with
`redisx.ContextTimeoutEnabled(rdb)` and refuse a client that ignores ctx
deadlines.

### Inspecting the options

`Config.UniversalOptions()` returns the go-redis options `Open` builds.

```go
uo, err := redisx.Config{Addrs: []string{"redis-0:6379"}, Cluster: true}.UniversalOptions()
if err != nil {
	panic(err)
}
fmt.Println(uo.IsClusterMode, uo.ContextTimeoutEnabled)
```

Its doc comment suggests tweaking the options and calling
`redis.NewUniversalClient` yourself. D7 lint forbids that outside `redisx`
(tests excepted), and the result would skip tracing, metrics and the ping.
If you need a field `Config` does not expose, add it to `Config`. Never turn
off `ContextTimeoutEnabled`: `cache` and `ratelimit/redis` rely on it to
bound each call.

### Dropping per-command spans

A hot path with thousands of commands per request floods traces. Keep the
metrics and drop the spans:

```go
// Metrics stay on; per-command spans are dropped (hot paths, noisy traces).
rdb, err := redisx.Open(context.Background(), redisx.Config{Addrs: []string{mr.Addr()}}, redisx.WithoutTracing())
```

## Testing

Use miniredis. It runs in-process, needs no Docker and works with `Open`:

```go
mr, _ := miniredis.Run()
defer mr.Close()

rdb, err := redisx.Open(context.Background(), redisx.Config{Addrs: []string{mr.Addr()}})
```

- In `testing.T` tests, `miniredis.RunT(t)` closes the server for you.
- `mr.RequireAuth("secret")` tests `Password`.
- Pass `WithTracerProvider` / `WithMeterProvider` with noop providers to keep
  tests independent of the globals.
- miniredis is a single node. Test `ForEachShard` with a `redis.NewRing` over
  two miniredis servers (tests may build clients directly). Test hash-slot
  rules against a real cluster.
- Fast-forward TTLs with `mr.FastForward(d)` instead of sleeping.

## Observability

- **Spans:** one client span per command (and per pipeline) from redisotel,
  unless `WithoutTracing`.
- **Metrics:** redisotel connection-pool and command-duration metrics, always
  on.
- **Logs:** none. `redisx` returns errors instead.
- **Errors:** `Open` returns `redisx: ...` errors (no address, tracing or
  metrics setup, ping). Commands return go-redis errors; `redis.Nil` means a
  missing key.

## Production checklist

- Open the client once in `main` with `redisx.Open`, share it, close it last.
- Set `TLS` and credentials for any Redis outside your own network.
- Load the password from a secret, never from code.
- Use `{hash tags}` for multi-key commands when `IsCluster` is true.
- Use `ForEachShard` for `SCAN`, `FLUSHDB` and `INFO` on a cluster.
- Give every Redis call a ctx deadline; `Open` makes go-redis honour it.
- Size `PoolSize` for your concurrency.
- Do not call `redis.NewClient` or `redis.NewUniversalClient` (D7 lint).

## Migrating from the old kitchen

`storage`'s `config.Redis{...}` plus `redis.NewClient` becomes `redisx.Open`.
See [docs/migration/data-utils.md](../docs/migration/data-utils.md).
