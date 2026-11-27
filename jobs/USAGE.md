# jobs

Durable background jobs stored in the service's own SQL database: enqueue
a job in the same transaction as the business write, run it later in a worker
process with retries and a dead-letter state, and run periodic jobs once
across all replicas. One engine works on PostgreSQL and MySQL 8. It is the
canonical background-job package (D7 in
[docs/decisions.md](../docs/decisions.md)); do not use River, asynq,
machinery or a hand-written queue table. To publish messages to a broker use
[`pubsub`](../pubsub/USAGE.md) and its outbox instead: `jobs` runs code,
`pubsub` moves messages.

## Install

The packages are in the core module:

```sh
go get github.com/dungnh3/kitchen
```

- `jobs`: the engine.
- `jobs/pgx`: PostgreSQL driver (on a pool from `pgx.Open`).
- `jobs/mysql`: MySQL 8.0.1+ driver (on a `*sql.DB` from `mysqlx.Open`).
- `jobs/jobstest`: in-memory driver for tests.

## Quick start

Create the table in a migration. The driver gives you the SQL:

```go
drv, _ := jobspgx.New(pool, jobspgx.WithTable("jobs"))
fmt.Println(drv.Schema()) // paste into a goose migration, or use zen
```

Define the job, its worker, and the client (from `Example`):

```go
type SendWelcome struct {
	UserID string `json:"user_id"`
}

func (SendWelcome) Kind() string { return "send_welcome" }

reg := jobs.NewRegistry()
err := jobs.Register(reg, jobs.WorkFunc[SendWelcome](func(ctx context.Context, j *jobs.Job[SendWelcome]) error {
	return mailer.SendWelcome(ctx, j.Args.UserID) // must be idempotent
}))

client, err := jobs.NewClient(drv, jobs.WithRegistry(reg), jobs.WithLogger(logger))
```

Enqueue inside the business transaction (PostgreSQL; MySQL takes a `*sql.Tx`):

```go
err = jpgx.BeginFunc(ctx, pool, func(tx jpgx.Tx) error {
	// ... business writes with tx ...
	_, err := client.InsertTx(ctx, tx, SendWelcome{UserID: id}, nil)
	return err
})
```

Run workers in the worker process, next to the other runners:

```go
err = client.Run(ctx) // returns nil after ctx is cancelled and running jobs settled
```

Use one client without a registry in the API process: it inserts and never
runs. Defaults: queue `default` with 10 workers, 20 attempts with a backoff
from 1s to 1h, a 1 minute timeout per attempt, a 30s lease, clean-up after
24 hours (completed) and 7 days (cancelled, discarded).

## Configuration

| Option | Default | Change it when |
|---|---|---|
| `WithRegistry(r)` | none (insert only) | the process runs jobs |
| `WithQueues(Queue{Name, Workers})` | `default`, 10 | you need isolation or less concurrency per queue |
| `WithMaxAttempts(n)` | 20 | a job should give up sooner |
| `WithRetryBackoff(base, max)` | 1s, 1h | retries must be faster or slower |
| `WithJobTimeout(d)` | 1m (`<= 0`: none) | attempts run longer; a worker's `Timeout()` overrides it |
| `WithLease(d)` | 30s | crashed jobs must come back sooner (or the database pauses longer) |
| `WithShutdownTimeout(d)` | 30s | jobs take longer to finish at deploy |
| `WithPollInterval(d)` | 1s | MySQL latency matters (PostgreSQL wakes on NOTIFY) |
| `WithRetention(done, dead)` | 24h, 7d | you keep history longer or shorter |
| `WithPeriodic(Every(d, newArgs))` | none | recurring work |
| `WithLogger`, `WithTracerProvider`, `WithMeterProvider`, `WithPropagator` | `log.Default()`, OTel globals | tests or custom wiring |

Per-job options go in `InsertOpts` (or on the args, by implementing
`InsertOpts() jobs.InsertOpts`): `Queue`, `Priority` (lower first),
`MaxAttempts`, `ScheduledAt`, `UniqueKey`, `KeepUnique`.

## What a worker returns

| Return | Result |
|---|---|
| `nil` | completed |
| an error | retried after the backoff; discarded after `MaxAttempts` |
| `jobs.Cancel(err)` | cancelled at once, not retried |
| `jobs.Snooze(d)` | runs again after `d`; the attempt is not counted |
| a panic | logged with the stack, counted as a failed attempt |

Cancelled and discarded jobs stay in the table for the retention period with
their last ten errors, so you can read them (`client.Get(ctx, id)`) and, if
you want, enqueue the job again.

## Integration with kitchen

**Logging.** Pass the `*log.Logger`. Every record has `job_id`, `job_kind`,
`queue` and `attempt`. A discard is logged at Error, a failed attempt at
Warn. Args are never logged.

**Worker binary.** Run the client next to the other runners and stop it
with the context (this is what `zen new service -with jobs` generates):

```go
runners = append(runners, client.Run)
```

**Telemetry.** An insert starts a `jobs.insert` span and stores the trace
context in the row. The attempt is a `jobs.work <kind>` span, a child of the
enqueuer's span, so a trace runs from the request through the job.

**Migrations.** `Driver.Schema()` is idempotent SQL. Put it in a goose
migration run through `migrate`.

## Advanced

### Several job tables in one service

Each driver has its own table, and each table has its own client, queues,
retention and workers. Use it to keep a noisy job type from bloating the
table of another, or to give each its own retention.

```go
emails, _ := jobspgx.New(pool, jobspgx.WithTable("emails_jobs"))
reports, _ := jobspgx.New(pool, jobspgx.WithTable("reports_jobs"))
```

For plain isolation or concurrency limits, queues in one table are cheaper.

### Unique and scheduled jobs

`UniqueKey` makes an insert a no-op while a job with that key exists
(`res.Duplicate`). The key is released when the job reaches a final state;
`KeepUnique` holds it until the clean-up deletes the row, which makes a
job once-per-key for the retention period.

### Periodic jobs

`jobs.Every(d, newArgs)` runs at every multiple of `d` counted from the Unix
epoch: `24*time.Hour` is 00:00 UTC. Every replica inserts the next slot's
job and a unique key lets one insert win, so a run happens once. Each run is
a normal job, so it has retries and a history. A slot that passes while every
replica is down is skipped. There are no cron expressions.

### At-least-once and idempotency

A job can run twice: after a crash, after a lost lease, or when recording the
result failed. Make the worker idempotent, for example by keying its side
effect on `j.ID` or on a business key.

### How crashes are recovered

A claim is a lease (`WithLease`) that a heartbeat renews every third of the
lease. A worker that dies stops renewing; after the lease the job is claimed
again and the attempt counts. A job that dies on its last attempt is
discarded by the clean-up, and the discard is logged. Every write that records
a result carries the claim's fencing counter, so a worker that was only
paused cannot overwrite the result of the one that took over; its job
context is cancelled with `jobs.ErrClaimLost`.

## Testing

`jobstest.New()` is an in-memory driver with the same rules, on the process
clock, so a test can use `testing/synctest`:

```go
synctest.Test(t, func(t *testing.T) {
	drv := jobstest.New()
	client, _ := jobs.NewClient(drv, jobs.WithRegistry(reg))
	go client.Run(ctx)
	client.Insert(ctx, SendWelcome{UserID: "u-1"}, nil)
	synctest.Wait()
	rows := drv.Rows() // assert state, attempts and errors
})
```

It has no transactions: use a database for tests of the transactional insert
(`KITCHEN_TEST_PG_DSN`, or testcontainers as the driver tests do).

## Observability

Metrics: `kitchen.jobs.inserted`, `kitchen.jobs.finished` (attribute
`kitchen.jobs.outcome`: completed, retried, snoozed, cancelled, discarded,
released, lost), `kitchen.jobs.duration` and `kitchen.jobs.queue_lag` (time
past the scheduled time before a worker claimed the job), all by kind and
queue. Alert on a growing queue lag and on any `discarded`.

## Production checklist

- Run the migration before the first deploy of the worker.
- Make every worker idempotent.
- Set `WithJobTimeout` (or a worker `Timeout()`) below the time you would
  wait for a stuck job.
- Give `WithShutdownTimeout` enough time for a normal job, and set the
  orchestrator's grace period above it.
- Alert on `discarded` jobs and on queue lag.
- Keep the job table in the same database as the business data to keep
  the transactional insert; do not point two services at one table.
- On MySQL open the database with `mysqlx.Open` (UTC, `parseTime`).

## Migrating from the old kitchen

There was no job queue. Services that ran periodic work with `lease.Elector`
can move it to `jobs.Every`; see [docs/migration/jobs.md](../docs/migration/jobs.md).
