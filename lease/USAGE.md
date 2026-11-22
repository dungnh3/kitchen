# lease

`lease` gives named, expiring locks with fencing tokens, and a leader
elector built on them. Use `lease.Do` for a mutex across processes and
`lease.Elector` for "one leader runs this". Do not use `SETNX`, redislock
or redsync. It is the canonical distributed lock package
([D7](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22)).

The core module has the `Locker` interface, `Do` and `Elector`. The
`lease/redis` submodule implements `Locker` on Redis.

## Install

```sh
go get github.com/dungnh3/kitchen             # lease, redisx
go get github.com/dungnh3/kitchen/lease/redis # Redis Locker
```

## Quick start

Open Redis with `redisx.Open`, build a Locker, and run work under a lease:

```go
ctx := context.Background()
mr, _ := miniredis.Run() // in a service: your Redis address
defer mr.Close()
rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
if err != nil {
	panic(err)
}
defer func() { _ = rdb.Close() }()

locker, err := leaseredis.New(rdb)
if err != nil {
	panic(err)
}
logger := log.Discard() // in a service: the *log.Logger from main

err = lease.Do(ctx, locker, "invoice-batch", 30*time.Second, func(_ context.Context, l lease.Lease) error {
	// Only one process runs this at a time. Long work must watch ctx.
	fmt.Println("running with token", l.Token)
	return nil
}, lease.DoLogger(logger))
```

What `Do` does for you:

- It returns `ErrNotHeld` at once if another owner holds the lease. It
  does not wait.
- It renews the lease every `ttl/3` while `fn` runs.
- If the lease is lost, or would expire before a renewal, `fn`'s context
  is cancelled with cause `ErrLeaseLost`, and `Do` returns an error
  wrapping it.
- It releases the lease when `fn` returns, only if this owner still holds
  it. A panic in `fn` is returned as an error.
- Every acquisition gets a new, strictly larger fencing token,
  `Lease.Token`.

## Configuration

`lease.Do(ctx, locker, name, ttl, fn, opts...)`: `ttl` must be at least
30ms.

| Option | Default | When to change |
|---|---|---|
| `DoOwner(id)` | `NewOwnerID()` (`<hostname>-<random>`) | stable owner names in logs |
| `DoLogger(l)` | `log.Default()` | pass the `*log.Logger` from `main` |

`lease.NewElector(locker, name, opts...)`:

| Option | Default | When to change |
|---|---|---|
| `WithTTL(d)` | 15s (renewed every TTL/3) | faster failover (shorter) or fewer Redis calls (longer) |
| `WithRetryInterval(d)` | TTL/3 | how often a follower tries to take over |
| `WithOwner(id)` | `NewOwnerID()` | stable owner names |
| `WithLogger(l)` | `log.Default()` | pass the `*log.Logger` from `main` |

`leaseredis.New(rdb, opts...)` takes `WithPrefix(p)` (default `lease:`).
Names are at most 200 bytes and must not contain `{`, `}`, spaces or
newlines.

## Integration with kitchen

### Redis via redisx

`leaseredis.New` takes any `redis.UniversalClient`. Create it with
`redisx.Open`, never `redis.NewClient`. You own the client and close it.

### Logger

Pass the process logger with `DoLogger` or `WithLogger`. `main` calls
`log.Setup` once. The elector logs leadership changes at Info and failed
renewals at Warn, with `lease` and `token` attributes.

### Errors and HTTP status

Both errors are `errs` errors with code Aborted, so a kitchen server
answers 409 "try again":

| Error | Reason | Returned when |
|---|---|---|
| `ErrNotHeld` | `LEASE_NOT_HELD` | another owner holds the lease (`Acquire`, `Do`), or a renewal found it lost |
| `ErrLeaseLost` | `LEASE_LOST` | the lease was lost while `fn` or the leader ran |

```go
// Another process holds the lease.
_, _ = locker.Acquire(ctx, "invoice-batch", "pod-a", time.Minute)

err := lease.Do(ctx, locker, "invoice-batch", time.Minute, func(context.Context, lease.Lease) error {
	return nil // not called
}, lease.DoOwner("pod-b"))
fmt.Println(errors.Is(err, lease.ErrNotHeld), errs.HTTPStatus(err)) // true 409
```

Return them from a handler as they are. Inside `fn`, use
`context.Cause(ctx)` to see whether the cancellation was `ErrLeaseLost`.

### Server wiring

Start an elector in a goroutine from `main` with the server's root
context, so it stops on shutdown. Full service wiring is in
[server/USAGE.md](../server/USAGE.md).

## Advanced

### Leader election

Use an `Elector` when one process at a time must run a loop: a scheduler,
an outbox relay, a consumer that must not run twice. `Run` campaigns until
`ctx` is cancelled, then returns nil. While this process leads, `lead`
runs with a context that is cancelled when the lease is lost or `ctx`
ends. When `lead` returns, the lease is released and the campaign starts
again.

```go
elector, err := lease.NewElector(locker, "billing-scheduler",
	lease.WithTTL(15*time.Second),
	lease.WithLogger(log.Discard()), // in a service: the *log.Logger from main
)
if err != nil {
	panic(err)
}
err = elector.Run(ctx, func(ctx context.Context, l lease.Lease) error {
	fmt.Println("leader, token", l.Token)
	cancel() // here: shutdown. Normally lead runs until ctx is done.
	<-ctx.Done()
	return nil
})
```

`lead` must return promptly once its context is done. The lease is
released only after it returns, and `lead` never runs twice at once in one
process. To split work, take one lease per shard name
(`orders-shard-3`).

### Fencing tokens

A lease is not a perfect mutex. A process can pause (GC, a slow disk, a
network partition) for longer than the TTL. Another process then takes
the lease, and the first one wakes up still thinking it holds it. Redis
replication is also asynchronous: after a failover, a lease can be
granted twice.

The fencing token closes that gap. Pass `l.Token` to every write you
protect. The resource stores the highest token it has seen and rejects
lower ones:

```go
// fencedStore stands for the resource a lease protects (a database row, a
// file). It rejects writes with a token older than the newest it has seen.
type fencedStore struct{ lastToken uint64 }

var errStaleToken = errors.New("stale fencing token")

func (s *fencedStore) Write(token uint64) error {
	if token < s.lastToken {
		return errStaleToken
	}
	s.lastToken = token
	return nil
}
```

```go
// Pod A takes the lease, then pauses (GC, network) past its TTL.
a, _ := locker.Acquire(ctx, "report", "pod-a", time.Second)
mr.FastForward(2 * time.Second)

// Pod B takes over and writes with a newer token.
b, _ := locker.Acquire(ctx, "report", "pod-b", time.Second)
fmt.Println("B:", store.Write(b.Token))

// Pod A wakes up and tries to write: the store refuses the old token.
fmt.Println("A:", errors.Is(store.Write(a.Token), errStaleToken))
```

In SQL this is a condition such as
`UPDATE jobs SET ..., fence = $token WHERE id = $id AND fence <= $token`.

### Renewals and TTL choice

`Do` and the elector renew every TTL/3. If a renewal fails with a Redis
error, they log it and try again at the next tick. They cancel the work
before the lease can expire (with a TTL/5 safety margin), even if Redis
never answers. So:

- A shorter TTL means faster failover after a crash, and more Redis calls.
- The TTL must be longer than a few Redis round trips. 10–30s suits most
  jobs.
- Work inside `fn` must watch `ctx` and stop when it is done.

### Using the Locker directly

`Locker` has three atomic, owner-checked calls: `Acquire`, `Renew` and
`Release`. Prefer `Do` or `Elector`: they renew, cancel and release for
you. Use `Locker` directly only for custom protocols, and always pass the
token on.

## Backends and adapters

### lease/redis

`leaseredis.New(rdb)` implements `Locker` on Redis (standalone, Cluster or
Sentinel).

- The lease is the key `<prefix>{<name>}`, holding `<owner>|<token>`, with
  a millisecond TTL.
- The token comes from `INCR` on `<prefix>{<name>}:fence`, which never
  expires, so tokens strictly increase per name. Do not delete those keys.
- Acquire, Renew and Release are Lua scripts, so every check-and-set is
  atomic. The hash tag keeps both keys in one Cluster slot.
- Redis replication is asynchronous. After a failover, a lease can be
  held twice. Always use the fencing token.

## Testing

There is no exported in-memory `Locker`. Use miniredis with
`lease/redis`, as in the examples above. `mr.FastForward(d)` expires
leases without sleeping. Pass `log.Discard()` with `DoLogger` or
`WithLogger`. Assert with `errors.Is(err, lease.ErrNotHeld)` and
`errors.Is(err, lease.ErrLeaseLost)`.

For unit tests of your own code, a small fake that implements the three
`Locker` methods is enough. Use `testing/synctest` for timing, not
`time.Sleep`.

## Observability

- **Logs** (from the elector and `Do`, with `lease` and `token`
  attributes): "acquired leadership", "released leadership" (Info);
  "leadership lost", "renew failed", "release failed", "acquire failed"
  (Warn); "leader returned an error" (Error).
- **Spans and metrics:** none of its own. Redis commands are traced by
  `redisx`.
- **Errs reasons:** `LEASE_NOT_HELD`, `LEASE_LOST` (both Aborted, 409).

## Production checklist

- Open Redis with `redisx.Open`.
- Pass `Lease.Token` to every protected write, and reject stale tokens.
- Make work inside `fn` and `lead` stop when `ctx` is done.
- Use a TTL of 10–30s; never below a few Redis round trips.
- Pass the `*log.Logger` from `main`.
- Give each shard its own lease name.
- Never delete the `:fence` keys.

## Migrating from the old kitchen

`SetNX` used as a lock becomes `lease.Do` with `lease/redis`. See
[data-utils.md](../docs/migration/data-utils.md#cache--cache).
`coordinators` becomes `lease.NewElector(...).Run`; see
[messaging.md](../docs/migration/messaging.md#requestreply-leases).
