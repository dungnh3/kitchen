# ratelimit

`ratelimit` limits request rates with GCRA buckets, kept in memory or
shared through Redis. Every request is counted against its most trusted
identity: a signed-in user, an anonymous subject, a device id, or else its
client IP. It is the canonical rate-limiting package (D7 in
[docs/decisions.md](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22));
do not use `x/time/rate`, `redis_rate` or another limiter in service code.

## Install

```sh
go get github.com/dungnh3/kitchen                 # ratelimit (in-memory store)
go get github.com/dungnh3/kitchen/ratelimit/redis # shared store for several instances
```

## Quick start

Build a limiter and install it on the server. With `DefaultLimits`, every
method without its own policy is limited by caller
(`Example_server` in `example_more_test.go`):

```go
// Users, anonymous subjects and devices 20/s each, unauthenticated
// client IPs 300/s, for every method without its own policy.
limiter, err := ratelimit.New(ratelimit.Config{
	Default: &ratelimit.Policy{Limits: ratelimit.DefaultLimits()},
})
if err != nil {
	panic(err)
}
// The client IP is only trusted from these proxies (the load balancer).
proxies, err := xcontext.NewTrustedProxies("10.0.0.0/8")
if err != nil {
	panic(err)
}
srv, err := server.New(
	// server.WithAuth(authn) first: signed-in callers are then limited
	// per user, everyone else per client IP.
	server.WithRateLimit(limiter),
	server.WithTrustedProxies(proxies),
	server.WithServices( /* your services */ ),
)
```

What the defaults give you:

- **One limiter for gRPC, REST and plain HTTP.** `server` runs it once,
  after authentication. The gRPC health service is never limited.
- **The right identity.** A signed-in user (`xcontext.UserID`, set by `auth`)
  is limited per user. Users behind a shared NAT do not share a bucket. An
  unauthenticated caller is limited by its device id *and* its client IP, so
  rotating fake device ids does not earn extra quota.
- **A safe client IP.** The IP comes from `server`, resolved through
  `WithTrustedProxies`. A raw `X-Forwarded-For` is never trusted.
- **Standard denials.** A denied call returns `RATE_LIMITED`
  (`ResourceExhausted`, HTTP 429) with `RetryInfo` and `Retry-After`.
- **Standard headers.** Every response carries `RateLimit-Policy`,
  `RateLimit` and `X-RateLimit-*`.
- **In memory, per instance.** That is fine for a single instance. With
  several instances, use the Redis store (see below).

`DefaultLimits` is a starting point, not tuned values. Roll it out with
`DryRun` first (see [Advanced](#roll-out-new-limits-with-dryrun)).

## Configuration

A policy names the thing it limits. That is a gRPC full method
(`/pkg.Svc/Method`), an HTTP route pattern (`POST /v1/chat`), or any name
you pass to `Check` or `Allow`. Each `Limit` in a policy counts one kind of
identity (`By`). A request must pass every limit that applies to it.

| `By` | Counts | Use it for |
|---|---|---|
| `ByCaller` (or `Policy.Rule`) | the request's own tier: user, anon, device or IP | one rule for everyone |
| `ByUser` | verified users only | per-account quotas (100/day) |
| `ByAnon` | verified anonymous subjects only | guest tokens |
| `ByDevice` | requests with only an unverified device id | pair it with `ByIP` |
| `ByIP` | unauthenticated requests, by client IP (IPv6 by `IPv6Prefix`, /64 by default) | loose, NAT-friendly cap |
| `ByAnyIP` | every request by client IP | abuse cap that also covers users |
| `ByGlobal` | all callers in one bucket | protect a fragile dependency |

| `Config` field | Default | Change it when |
|---|---|---|
| `Default` | nil: methods without a policy are not limited | you want a catch-all (`&Policy{Limits: DefaultLimits()}`) |
| `Policies` | none | a method needs its own buckets or rules |
| `Skip` | none | an endpoint must never be limited (internal ping) |
| `Store` | in memory, per instance | you run more than one instance: `RemoteStore(redis store)` |
| `Instances` | 1 | with a shared store: the fleet size, used while the store is down |
| `Subject`, `Anonymous`, `Device` | `xcontext.UserID`, none, `xcontext.Info.DeviceID` | your identity lives elsewhere |
| `Exempt` | none | trusted networks or accounts (see warning below) |
| `Overrides` | none | one account needs a different rule |
| `DryRun`, `Disabled` | false | roll-out and emergencies (see warnings below) |
| `OnDecision` | none | custom alerts or audit logs |
| `StoreTimeout`, `BreakerCooldown` | 100ms, 1s | your Redis is slower or farther away |
| `LocalMaxKeys`, `Prefix` | 100 000, `"rl"` | very many callers per instance; a shared Redis |
| `IPv6Prefix` | 64 | clients rotate addresses across their delegated /56 or /48 to get fresh IP buckets (32 to 128) |
| `Errors`, `Logger`, `MeterProvider` | `errs.WriteHTTP`, `log.Default()`, global | only for `Middleware` without `server`, and in tests |

A policy with several limits, plus `Exempt`, `Overrides` and `Skip`
(`Example` in `example_test.go`):

```go
l, err := ratelimit.New(ratelimit.Config{
	// Users, devices and anonymous subjects 20/s each, unauthenticated
	// IPs 300/s, buckets shared by every method without its own policy.
	Default: &ratelimit.Policy{Limits: ratelimit.DefaultLimits()},
	Policies: map[string]ratelimit.Policy{
		// 2/s per caller, and at most 100 per day per user.
		"/agent.v1.AgentService/Chat": {Limits: []ratelimit.Limit{
			{Rule: ratelimit.Rule{Rate: 2, Per: time.Second}, By: ratelimit.ByCaller},
			{Rule: ratelimit.Rule{Rate: 100, Per: 24 * time.Hour}, By: ratelimit.ByUser},
		}},
	},
	Exempt:    []string{"ip:10.0.0.0/8"},                                               // internal traffic
	Overrides: map[string]ratelimit.Rule{"user:partner": {Rate: 50, Per: time.Second}}, // a bigger caller
	Skip:      []string{"/agent.v1.AgentService/Ping"},
})
```

> **Warning: `Exempt`.** An exempt identity is never limited. Exempt only
> networks you control (`ip:10.0.0.0/8`) or named accounts. Device ids are
> client-controlled, so `New` rejects `device:` entries. An `ip:` entry is
> only as trustworthy as `server.WithTrustedProxies`.

## Integration with kitchen

- **server.** `server.WithRateLimit(l)` installs the gRPC interceptors (by
  full method) and the plain-HTTP check (by route pattern). It forwards the
  rate-limit headers through the REST gateway. Denials are rendered by the
  server's `errs.Handler`, localized like every other error. See the Quick
  start.
- **auth and xcontext.** `auth` stores the verified subject with
  `xcontext.WithUserID`, and that is the user ratelimit counts. Install
  `WithAuth` together with `WithRateLimit`; `server` orders them for you.
  The device id is `X-Device-Id` (`xcontext.Info.DeviceID`). It is hashed in
  bucket keys.
- **errs.** A denial is `ratelimit.ErrRateLimited` (`errs.ErrRateLimited`)
  with a `RetryInfo` detail and `policy` metadata. With `OnStoreError: Deny`
  the error is `ratelimit.ErrUnavailable` (`RATE_LIMITER_UNAVAILABLE`, 503).
- **redisx.** The Redis store takes a client from `redisx.Open`. See
  [Backends and adapters](#backends-and-adapters).
- **log and otelx.** Store outages are logged through `Config.Logger`
  (`log.Default()` when nil): one warning when the breaker opens and per
  failed probe, one info line when the store recovers, never per request. Metrics use the global `MeterProvider` that
  `otelx/sdk` installs.

### Limits inside business logic

`Check` runs a named policy against the caller in `ctx`. Use it for actions
that are not one RPC, like sending an OTP. `Allow` takes a bucket key you
choose, for example one per tenant (`ExampleLimiter_Allow`):

```go
l, err := ratelimit.New(ratelimit.Config{
	Policies: map[string]ratelimit.Policy{
		"export": {Rule: ratelimit.Rule{Rate: 2, Per: time.Hour}},
	},
})
if err != nil {
	panic(err)
}
// A bucket chosen by the caller, here one per tenant. Keys live in their
// own namespace, so they never drain a user or IP bucket.
for range 3 {
	d, _ := l.Allow(context.Background(), "export", "tenant:42")
	fmt.Println(d.Allowed)
}
```

The `Decision` is always usable. A non-nil error only reports a store
failure that `OnStoreError` already handled.

### Plain net/http without server

`Middleware` looks up the policy by `r.Pattern`, so wrap handlers that are
registered on a `ServeMux` (`ExampleLimiter_Middleware`):

```go
mux := http.NewServeMux()
mux.Handle("GET /hello", l.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	_, _ = fmt.Fprint(w, "hi")
})))
```

The second request in one minute gets `429`, `Retry-After: 60` and
`RateLimit: "GET /hello";r=0;t=60`. To render the error yourself, call
`LimitHTTP` and pass its error to your `errs.Handler`
(`ExampleLimiter_LimitHTTP`).

## Advanced

### Per-tenant and other custom keys

`Limit.Key` replaces `By` with your own bucket key, read from the context.
Combine it with a per-caller limit (`ExampleLimit_key`):

```go
"/search.v1.Search/Query": {Limits: []ratelimit.Limit{
	// Every tenant shares 1 000/min across its users...
	{Rule: ratelimit.Rule{Rate: 1000, Per: time.Minute}, Key: func(ctx context.Context) (string, bool) {
		t, ok := ctx.Value(tenantKey{}).(string)
		return t, ok
	}},
	// ...and each caller still gets at most 10/s.
	{Rule: ratelimit.Rule{Rate: 10, Per: time.Second}, By: ratelimit.ByCaller},
}},
```

Limits are checked from the most specific bucket to the most shared, and
checking stops at the first denial. A denied request therefore does not use
up the tenant, IP or global buckets.

### Roll out new limits with DryRun

You need this before enforcing any new or tightened limit. `DryRun` counts
and reports limited requests but never blocks them. Watch the `dry_run`
results, then remove the flag (`ExampleConfig_dryRun`):

```go
l, err := ratelimit.New(ratelimit.Config{
	Policies: map[string]ratelimit.Policy{
		"send-otp": {Rule: ratelimit.Rule{Rate: 1, Per: time.Minute}},
	},
	// Count and report, never block: roll new limits out like this and
	// read the "dry_run" results before enforcing.
	DryRun: true,
	OnDecision: func(_ context.Context, e ratelimit.Event) {
		fmt.Println(e.Policy, e.Limit, e.Result)
	},
})
```

> **Warning: `DryRun` and `Disabled`.** A dry-run policy blocks nothing,
> and `Disabled` turns the limiter off entirely. Both remove your
> protection against brute force and overload. Use `DryRun` per policy
> (`Policy.DryRun`) during a roll-out, keep it short, and alert on it.
> Treat `Disabled` as an emergency switch only.

### When the store is down

A failing remote store trips a circuit breaker. Each policy's
`OnStoreError` then decides what happens (`ExamplePolicy_onStoreError`):

```go
Policies: map[string]ratelimit.Policy{
	// Login brute force must stay blocked while Redis is down.
	"/auth.v1.Auth/Login": {Rule: ratelimit.Rule{Rate: 5, Per: time.Minute}, OnStoreError: ratelimit.Deny},
	// Search degrades to a per-instance limit (the default).
	"/search.v1.Search/Query": {Rule: ratelimit.Rule{Rate: 10, Per: time.Second}},
},
```

- `FallbackLocal` (the default) answers from an in-memory bucket whose rule
  is divided by `Config.Instances`, so the fleet stays near the configured
  rate. Decisions carry `Degraded: true`.
- `Deny` fails closed with `RATE_LIMITER_UNAVAILABLE`. Use it for login,
  OTP and payment endpoints.
- `Allow` fails open.

> **Warning: `OnStoreError: Allow`.** Failing open means a Redis outage
> removes every limit on that policy. Choose it only for endpoints where
> availability matters more than abuse protection. `DryRun` policies never
> block during an outage either.

## Backends and adapters

### ratelimit/redis

You need this whenever more than one instance serves the same API: every
instance then uses the same buckets. The store runs one atomic GCRA Lua
script (EVALSHA, falling back to EVAL). It works with standalone, Sentinel
and Cluster clients. Times are integer microseconds from Redis `TIME`, so
every instance uses one clock and the decisions are exactly the in-memory
store's (a differential test drives both with the same random requests)
(`Example_limiter` in `ratelimit/redis/example_limiter_test.go`):

```go
// redisx.Open: traced, metered, ContextTimeoutEnabled already set.
rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
if err != nil {
	panic(err)
}
defer func() { _ = rdb.Close() }()
store, err := rlredis.New(rdb)
if err != nil {
	panic(err)
}
l, err := ratelimit.New(ratelimit.Config{
	Store:     ratelimit.RemoteStore(store), // buckets shared by every instance
	Instances: 4,                            // divides rules while Redis is down
	Policies: map[string]ratelimit.Policy{
		"send-otp": {Rule: ratelimit.Rule{Rate: 1, Per: time.Minute}},
	},
})
```

Limits:

- **Context timeouts.** `rlredis.New` refuses a client without
  `ContextTimeoutEnabled`. Without it, a hanging Redis would ignore the
  limiter's 100ms timeout. Clients from `redisx.Open` have it.
- **Shared Redis.** Set a distinct `Config.Prefix` per service when several
  services share one Redis. Every bucket key starts with it.
- **Redis Cluster.** A bucket is one key, so each script call touches one
  slot. `{`, `}`, `%` (and `:` in policy names) are percent-encoded in keys,
  so a route such as `GET /files/{id}` or a client-chosen id never forms a
  hash tag that would put many buckets in one slot. `Prefix` is used as is.
- **Breaker scope.** The breaker is one per Limiter, not per Cluster node:
  when one node fails, every bucket falls back to `OnStoreError` until a
  probe succeeds.
- **Rule limits.** A rule may not exceed one request per microsecond per
  bucket, and burst × interval must stay under 2^52 µs (about 142 years);
  `New` rejects such rules.
- **Ownership.** You own `rdb` and close it on shutdown.

## Testing

- **Defaults.** The default in-memory store needs no setup. Tests are
  deterministic, as in the examples above.
- **Redis.** Run the Redis store against `miniredis` (see `Example_limiter`).
- **Outages.** Simulate one with a `Store` that returns an error, as
  `ExamplePolicy_onStoreError` does.
- **Asserting on denials.** Use `errors.Is` and `errs` instead of matching
  strings (`ExampleLimiter_LimitHTTP`):

```go
r := httptest.NewRequest(http.MethodPost, "/v1/upload", nil)
_ = l.LimitHTTP(httptest.NewRecorder(), r, "POST /v1/upload")
err = l.LimitHTTP(httptest.NewRecorder(), r, "POST /v1/upload")
// A denial is an errs error: render it with errs.Handler.WriteHTTP.
fmt.Println(errors.Is(err, ratelimit.ErrRateLimited), errs.Code(err), errs.HTTPStatus(err))
// Output:
// true ResourceExhausted 429
```

Set the identity under test with `xcontext.WithUserID(ctx, "u1")`. Pass
`Logger: log.Discard()` to keep test output clean.

## Observability

Metrics (meter `github.com/dungnh3/kitchen/ratelimit`):

| Metric | Attributes | Meaning |
|---|---|---|
| `kitchen.ratelimit.decisions` | `policy`, `limit`, `result` | one per limited request. `result` is `allowed`, `limited`, `dry_run`, `degraded` or `unavailable` |
| `kitchen.ratelimit.store.errors` | none | failed calls to the remote store |

- **Headers.** `RateLimit-Policy` and `RateLimit` (IETF draft
  `draft-ietf-httpapi-ratelimit-headers` revisions 09 to 11:
  `"<id>";q=;w=` and `"<id>";r=;t=`, RFC 9651 lists; revision 12 renamed
  `r`/`t` to `a`/`w` and is not followed yet) list every limit that applied.
  `w` is whole seconds: a rule with a sub-second or fractional `Per` is
  scaled, so 1 per 100ms is `q=10;w=1`. `X-RateLimit-Limit`, `-Remaining`
  and `-Reset` describe the deciding one (`-Reset` is seconds until the
  bucket is full again, not a timestamp). `Retry-After` is set on 429 and on
  the 503 of `OnStoreError: Deny`.
- **Errors.** Reasons are `RATE_LIMITED` (429) and
  `RATE_LIMITER_UNAVAILABLE` (503), both with `policy` metadata.
- **Logs.** The breaker logs a warning when the store starts failing and
  an info line when it recovers.
- **Multiple limits.** Limits are checked from the most specific bucket to
  the most shared one and stop at the first denial. A request that a shared
  limit (IP, global) denies has already used a token of the specific ones
  (user, device); it is not refunded, which keeps denials to one store call
  each under a flood (Envoy behaves the same by default).
- **Events.** `OnDecision` receives the same policy, limit and result as the
  metric, plus the `Decision`.

## Production checklist

- Set `server.WithTrustedProxies` to your load balancer ranges. Without it,
  every caller behind the proxy looks like one IP.
- With more than one instance, use `ratelimit/redis` and set `Instances`.
- Use `OnStoreError: Deny` for login, OTP and payment policies.
- Roll out new limits with `Policy.DryRun`, then enforce.
- Keep `Exempt` to networks and accounts you control. Review `Overrides`.
- Alert on `result="unavailable"` and `"degraded"`, and on
  `kitchen.ratelimit.store.errors`.
- Do not set `Disabled` outside an incident.

## Migrating from the old kitchen

`rlimit` becomes `ratelimit`, and its Redis store becomes `ratelimit/redis`.
Identity tiers, IETF headers and fail-closed options are new. See
[docs/migration/errors-auth.md](../docs/migration/errors-auth.md#ratelimit-from-rlimit).
