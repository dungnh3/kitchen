# timex

Small time helpers that kitchen packages share:

- **retry backoff and jitter.** `Backoff` and `Jitter` are canonical
  (D7 in [docs/decisions.md](../docs/decisions.md)): never re-implement
  exponential backoff or random spreading in another package;
- **lenient parsing** of common date and time layouts;
- **calendar boundaries** that keep the input's time zone.

## Install

```sh
go get github.com/dungnh3/kitchen   # timex is in the core module
```

## Quick start

A reconnect loop that waits longer after each failure and stops when the
context ends (from `ExampleBackoff`):

```go
for attempt := 1; ; attempt++ {
	if err := connect(attempt); err == nil {
		fmt.Println("connected after", attempt, "attempts")
		return
	}
	t := time.NewTimer(timex.Backoff(time.Millisecond, 10*time.Millisecond, attempt))
	select {
	case <-ctx.Done():
		t.Stop()
		return
	case <-t.C:
	}
}
```

`Backoff(base, max, attempt)` returns `base·2^(attempt-1)` capped at `max`,
with "equal jitter": a uniform value between half and all of it, so many
clients that fail together do not retry together. A `base` of 0 or less
returns 0, a `max` below `base` is raised to `base`, and an `attempt` below
1 counts as 1.

## Configuration

`timex` has no options or environment variables. Each caller picks its own
bounds:

| Helper | Typical values | Notes |
|---|---|---|
| `Backoff(base, max, attempt)` | `1s, 30s` for handlers; `1s, 5m` for background relays | use a timer and select on `ctx.Done()`, never `time.Sleep` |
| `Jitter(d, frac)` | `frac` 0.1 (±10%) | `frac` is clamped to [0, 1]; 0 returns `d` unchanged |

## Integration with kitchen

`Backoff` drives every retry loop in kitchen:

- `pubsub` in-process retries (`WithRetry`);
- the `outbox` relay and the `outbox/pgx` listener;
- the Kafka redelivery delay and the SQS receive loop;
- `httpx` retries.

For HTTP calls, configure `httpx.WithRetry` instead of writing a loop; it
already uses `Backoff` and honours `Retry-After`.

`Jitter` spreads TTLs: `kitchen/cache` jitters entry lifetimes through it,
so keys written in a burst do not all expire in the same second (from
`ExampleJitter`):

```go
ttl := timex.Jitter(10*time.Minute, 0.1) // 9m–11m
fmt.Println(ttl >= 9*time.Minute && ttl <= 11*time.Minute)
// Output: true
```

Parse errors are `kitchen/errs` errors with code InvalidArgument:
`timex.ErrEmpty` (reason `EMPTY_TIME`) for a blank string, and a wrapped
`errs.ErrInvalidArgument` for an unknown layout. A handler can return them
as they are and the client gets a 400.

## Advanced

### Parsing user input

Use it when values come from forms, CSV files or query strings in mixed
formats. `Parse` tries RFC 3339, `2006-01-02 15:04:05`, `2006-01-02`,
`2006/01/02`, `15:04:05`, `2006-01` and `2006/01`, in that order. Values
without an offset are read as UTC; use `ParseInLocation` for a local zone
(from `ExampleParseInLocation`):

```go
ict := time.FixedZone("ICT", 7*3600)
t, _ := timex.ParseInLocation("2025-03-04 08:30:00", ict)
fmt.Println(t)
_, err := timex.Parse("  ")
fmt.Println(errors.Is(err, timex.ErrEmpty))
// Output:
// 2025-03-04 08:30:00 +0700 ICT
// true
```

`MustParse` panics on error. Keep it for constants and tests.

### Calendar boundaries in a time zone

Use it for daily or monthly reports. Every helper keeps the location of its
input: `StartOfDay` of a time in `Asia/Ho_Chi_Minh` is local midnight, not
UTC midnight (from `ExampleStartOfDay`):

```go
ict := time.FixedZone("ICT", 7*3600)
t := time.Date(2025, 3, 4, 1, 30, 0, 0, ict)
fmt.Println(timex.StartOfDay(t))
fmt.Println(timex.EndOfMonth(t).Format(time.RFC3339Nano))
// Output:
// 2025-03-04 00:00:00 +0700 ICT
// 2025-03-31T23:59:59.999999999+07:00
```

`End*` helpers return the last nanosecond of the period, so `t <= EndOfDay(t)`
holds for every instant of the day. The package has no default zone. Load
one with `time.LoadLocation` in `main`, and import `_ "time/tzdata"` when the
runtime image has no zoneinfo.

### Iterating over days

Use it to walk a date range, both ends included (from `ExampleDays`):

```go
from := time.Date(2025, 3, 30, 0, 0, 0, 0, time.UTC)
for d := range timex.Days(from, from.AddDate(0, 0, 2)) {
	fmt.Println(d.Format(time.DateOnly))
}
// Output:
// 2025-03-30
// 2025-03-31
// 2025-04-01
```

## Testing

`Backoff` and `Jitter` are random. Assert on bounds (`d >= base/2 && d <= max`),
not exact values. Timing loops that use them are easiest to test with
`testing/synctest`, which runs timers on a fake clock.

## Observability

`timex` emits no spans, metrics or logs. The errs reasons it returns are
`EMPTY_TIME` and `INVALID_ARGUMENT`.

## Production checklist

- Always cap `Backoff` with a `max`, and stop the loop on `ctx.Done()`.
- Use `httpx.WithRetry` for HTTP instead of a hand-written loop.
- Load time zones explicitly. Never rely on the host's local zone.

## Migrating from the old kitchen

`MustParseTime`, the old `StartOf*`/`EndOf*` helpers and `RangeDates` map to
`timex`; see [docs/migration/data-utils.md](../docs/migration/data-utils.md).
