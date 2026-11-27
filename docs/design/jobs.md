# Design: `jobs` (background job queue) and zen tasks

Status: accepted. The owner chose a custom engine over River so that the same
package works on PostgreSQL and MySQL, and approved this design (no leader,
core module, `zen new service -with jobs` and `zen add task`).

## 1. Why

Services need durable background work: send an email after a sign-up,
recompute a report, call a slow partner API, run something every 5 minutes.
Today kitchen has two partial answers:

- `lease.Elector` periodic jobs in the zen worker: one replica runs a
  function on a ticker. A failed run is lost, there is no retry and no
  history, and it needs Redis.
- `pubsub/outbox`: durable and transactional, but it moves messages to a
  broker; it does not run code.

`jobs` is the canonical package (D7) for durable jobs stored in the
service's own SQL database:

- enqueue in the same transaction as the business write, so a job exists
  if and only if the transaction committed;
- at-least-once execution with retries and backoff, then a dead-letter
  state (`discarded`) that keeps the job for inspection;
- unique jobs, scheduled jobs, periodic jobs;
- named queues with their own concurrency;
- several job tables per service (`orders_jobs`, `emails_jobs`, ...);
- PostgreSQL and MySQL 8.0.1+ (`SKIP LOCKED`, JSON functions), through one
  engine and one driver per database. MariaDB is not supported.

## 2. Decisions

| # | Question | Decision | Why |
|---|---|---|---|
| 1 | Engine | Custom, on `SELECT ... FOR UPDATE SKIP LOCKED` | Works on PostgreSQL and MySQL; follows kitchen conventions (`log`, `errs`, `timex`, `otelx`) end to end; no MPL dependency. River is Postgres-only. |
| 2 | Crash recovery | Claims are leases with a fencing counter (the `pubsub/outbox` model), renewed by a heartbeat every lease/3 while the job runs | No separate rescuer and no leader: an expired claim is simply claimable again, and a late settle from the old owner is a no-op. |
| 3 | Leader | None. Periodic enqueue and cleanup are idempotent and run on every replica | No Redis requirement and no leader table. Periodic runs are deduplicated by a unique key per time slot. |
| 4 | Periodic jobs | Persisted jobs (each run is a row, with retries) enqueued per slot | A failed run is retried and visible, unlike the ticker jobs. |
| 5 | Migrations | Plain SQL per dialect, rendered for the table name by `Driver.Schema()`; zen writes it into a goose file run by kitchen `migrate` | Readable, reviewable SQL; the table name is configurable. |
| 6 | Module | Core: `jobs`, `jobs/pgx`, `jobs/mysql` | Only light dependencies (jackc pgx and go-sql-driver are already in core), so D1 allows core; one tag. |
| 7 | zen | `zen new service -with jobs` (needs `-db postgres` or `-db mysql`) and `zen add task <name> [-every d]` | Same pattern as `zen add client`. |

## 3. Packages and API

```
jobs/          engine: Client, workers, retry, periodic, cleanup, telemetry
jobs/pgx       driver for kitchen pgx (import as jobspgx)
jobs/mysql     driver for kitchen mysqlx (import as jobsmysql)
jobs/jobstest  helpers for service tests (run one job inline, assert enqueued)
```

### Defining a job

```go
type SendWelcome struct {
	UserID string `json:"user_id"`
}

func (SendWelcome) Kind() string { return "send_welcome" } // stable, stored in the row

type SendWelcomeWorker struct{ mail Mailer }

func (w SendWelcomeWorker) Work(ctx context.Context, j *jobs.Job[SendWelcome]) error {
	return w.mail.Send(ctx, j.Args.UserID)
}
```

- `Args` is `interface{ Kind() string }`; args are stored as JSON.
- `Worker[T Args]` is `interface{ Work(ctx, *Job[T]) error }`. A worker may
  also implement `Timeout() time.Duration` (default 1m). Args may implement
  `InsertOpts() jobs.InsertOpts` for per-kind defaults (queue, priority,
  max attempts), which the insert call overrides.
- `jobs.WorkFunc[T](func(ctx, *Job[T]) error)` adapts a function.
- `Register[T](reg, worker)` stores the worker by `T`'s kind; a client only
  claims the kinds of its registry, so a job enqueued by a newer deploy waits
  for a worker that knows it instead of failing.
- `Job[T]` carries `ID`, `Kind`, `Queue`, `Args`, `Attempt`, `MaxAttempts`,
  `CreatedAt`, `ScheduledAt` and the errors of earlier attempts.

### Results a worker returns

| Return | Effect |
|---|---|
| `nil` | `completed` |
| any error | retried at `now + backoff(attempt)`; `discarded` after `MaxAttempts` |
| `jobs.Cancel(err)` | `cancelled` at once, no retry (permanent failure) |
| `jobs.Snooze(d)` | rescheduled after `d`; the attempt is not counted |
| `jobs.RetryAfter(d, err)` | retried after `d` instead of the backoff; the attempt counts (a kind with its own schedule, such as `webhook/delivery`) |
| panic | recovered, logged with the stack, counted as a failed attempt |
| context cancelled by shutdown | given back: available at once, attempt not counted |

Backoff is `timex.Backoff(1s, 1h, attempt)` with `timex.Jitter`, default
`MaxAttempts` 20 (about 20 hours in total). Both are options.

### Client

```go
drv, err := jobspgx.New(pool, jobspgx.WithTable("jobs"))         // or jobsmysql.New(db, ...)
reg := jobs.NewRegistry()
jobs.Register(reg, SendWelcomeWorker{mail})                       // generic, one per kind

client, err := jobs.NewClient(drv,
	jobs.WithRegistry(reg),                                       // omit for insert-only clients
	jobs.WithQueues(jobs.Queue{Name: "default", Workers: 10}),
	jobs.WithPeriodic(jobs.Every(5*time.Minute, func() jobs.Args { return Cleanup{} })),
	jobs.WithLogger(logger),
)
err = client.Run(ctx)   // worker process: fetch and run until ctx is cancelled
err = client.Close(ctx) // waits for running jobs, bounded by the shutdown timeout
```

`Client[TTx]` is generic over the driver's transaction type, so
`InsertTx` is type checked:

```go
// inside the business transaction (jpgx.Tx for jobspgx, *sql.Tx for jobsmysql)
res, err := client.InsertTx(ctx, tx, SendWelcome{UserID: id}, &jobs.InsertOpts{
	Queue:       "emails",
	ScheduledAt: time.Now().Add(time.Minute),
	UniqueKey:   "welcome:" + id,
})
// res.ID, res.Duplicate
```

`Insert` (no tx), `InsertMany`/`InsertManyTx` and `Get(ctx, id)` also exist.
An API process uses an insert-only client and never calls `Run`.

### Options (functional, no Config struct)

| Option | Default |
|---|---|
| `WithQueues(...Queue)` | `default` with 10 workers |
| `WithPollInterval(d)` | 1s (PostgreSQL also wakes on NOTIFY) |
| `WithLease(d)` | 30s; renewed every lease/3 while a job runs |
| `WithRetryBackoff(base, max)` | 1s, 1h |
| `WithMaxAttempts(n)` | 20 |
| `WithShutdownTimeout(d)` | 30s, then job contexts are cancelled |
| `WithRetention(completed, discarded)` | 24h, 7 days (cancelled as discarded) |
| `WithCleanupInterval(d)` | 1 minute |
| `WithJobTimeout(d)` | 1 minute |
| `WithPeriodic(...Periodic)` | none |
| `WithLogger`, `WithTracerProvider`, `WithMeterProvider`, `WithPropagator` | `log.Default()`, OTel globals |

Driver options: `WithTable(name)` (default `jobs`; each part matches
`[A-Za-z_][A-Za-z0-9_]{0,39}`, `schema.table` allowed, always quoted as an
identifier).

## 4. Storage

One table per job table name, same columns in both dialects:

| Column | Notes |
|---|---|
| `id` | bigint identity / auto increment |
| `kind`, `queue` | text |
| `args` | `jsonb` / `JSON` |
| `metadata` | `jsonb` / `JSON`: trace context of the enqueuer |
| `state` | `available`, `running`, `completed`, `cancelled`, `discarded` |
| `priority` | smallint, lower runs first (default 1) |
| `attempt`, `max_attempts` | int |
| `scheduled_at` | when it may run; DB time is the only clock used |
| `locked_until`, `claim` | lease end and fencing counter |
| `errors` | JSON array, the last 10 attempts, each message cut to 2 KiB |
| `unique_keep` | boolean: keep `unique_key` after the job ends |
| `unique_key` | nullable, unique index |
| `created_at`, `finalized_at` | timestamps |

Index for fetching: `(queue, state, priority, scheduled_at, id)`.

### Claim

PostgreSQL, one statement:

```sql
UPDATE jobs SET state='running', attempt=attempt+1, claim=claim+1,
       locked_until=now()+$lease
WHERE id IN (
  SELECT id FROM jobs
  WHERE queue=$1 AND ((state='available' AND scheduled_at<=now())
                   OR (state='running'   AND locked_until<now()))
  ORDER BY priority, scheduled_at, id
  LIMIT $n FOR UPDATE SKIP LOCKED)
RETURNING ...
```

MySQL has no `UPDATE ... RETURNING`: `SELECT ... FOR UPDATE SKIP LOCKED`
then `UPDATE ... WHERE id IN (...)` then a read of the rows, in one
`READ COMMITTED` transaction (no gap locks).

A job whose lease expired (the process crashed) is claimable again while it
has attempts left; the attempt counts. A job whose lease lapsed on its last
attempt is discarded by `Reap`, which the clean-up loop runs.

### Settle (fenced)

`Complete`, `Retry`, `Snooze`, `Cancel`, `Discard` and the heartbeat's
`Extend` all carry `(id, claim)` and change the row only while `claim`
matches. If the heartbeat finds the claim lost, the job's ctx is cancelled
and its result is ignored (another replica owns it). Settles use a
detached ctx with a short timeout, so a shutdown still records results.

### Unique jobs

`unique_key` has a unique index (NULLs do not collide in either dialect).
Insert uses `ON CONFLICT DO NOTHING` / `ON DUPLICATE KEY UPDATE id=id` and
reports `Duplicate: true`. By default the key is cleared when the job
finishes, so it only blocks duplicates while pending. `UniqueKeep: true`
keeps it until cleanup deletes the row (used by periodic jobs).

### Wake-ups

PostgreSQL: `NOTIFY <table>` in the insert, `LISTEN` in the worker
(like `outbox/pgx`), with polling as the fallback. MySQL: polling only.
An insert through a running client also wakes its own fetchers.

## 5. Periodic jobs and cleanup (leaderless)

- Every replica inserts the job of the next slot (the next multiple of the
  interval, scheduled at it) with `UniqueKey = "periodic:<kind>:<slot
  start, Unix seconds>"`, `KeepUnique: true`, and sleeps until that slot. Exactly one row per slot wins; the others are
  duplicates. Clock skew between replicas changes only which replica wins.
- Schedules are intervals (`jobs.Every(d, ...)`), aligned to the epoch.
  Cron expressions are out of scope (they would need a D7 decision on a
  cron library).
- Clean-up runs on every replica every minute: `Reap`, then `DELETE` of
  finished rows older than the retention, in batches of 1000 with
  `SKIP LOCKED` (PostgreSQL) or a locking `SELECT` first (MySQL), repeated
  while a batch is full.

## 6. Telemetry and logs

- A span per insert (producer) and per execution (consumer,
  `jobs.work <kind>`), linked through the trace context stored in
  `metadata`.
- Metrics: inserted, completed, retried, discarded, cancelled (counters by
  kind and queue); execution duration and queue lag (claim time minus
  `scheduled_at`) histograms.
- Logs through `log` with `job_id`, `job_kind`, `queue`, `attempt`, `max_attempts`.
  Failures at Warn, discards at Error. Args are never logged.

## 7. Lifecycle

`Run(ctx)` starts one fetch loop per queue, the periodic and clean-up loops,
the driver's listener and the heartbeat. After ctx is cancelled it stops
fetching, waits up to the shutdown timeout for running jobs (then cancels
them; they are given back without spending the attempt), stops the heartbeat
and returns. `Close(ctx)` cancels a running client and waits for `Run`. No goroutine outlives them
(goleak in every test).

## 8. Tests

- Engine with the in-memory driver `jobs/jobstest` under `testing/synctest`: retries,
  backoff, snooze, cancel, panic, shutdown, lease loss.
- Driver conformance suite (`jobs/internal/drivertest`) run against the
  in-memory driver and both SQL drivers (testcontainers, or
  `KITCHEN_TEST_PG_DSN` / `KITCHEN_TEST_MYSQL_DSN`): claim contention across clients, expired lease reclaim, fenced
  settles, unique keys, cleanup, several tables in one database.
- `Example*` tests for Register, InsertTx, periodic jobs.

## 9. zen

- `zen new service -with jobs` (needs `-db postgres` or `-db mysql`):
  - migration `NNNNN_create_jobs.sql` with the driver's `Schema()` SQL for
    the chosen dialect;
  - `internal/task/` with the registry, a sample periodic task and the
    marker `// zen:tasks`;
  - the API gets an insert-only client in its deps (`deps.jobs`); the worker
    runs a client with the registry and the periodic tasks (`internal/app/jobs.go`),
    with `JOBS_WORKERS` (default 10) workers on the `default` queue.
- `zen add task <name> [-queue q] [-every d]`: writes
  `internal/task/<name>.go` (typed args and worker) and its test, and
  registers it at `// zen:tasks`; with `-every` it also adds the periodic
  entry.
- `-with lease` and `zen add job` stay as they are (Redis leader jobs for
  services without a database).
- Golden files for `service-jobs-postgres`, `service-jobs-mysql` and
  `add-task`; `TestGeneratedServiceBuilds` covers both.

## 10. D7 and lint

D7 row: `| Background jobs | jobs (+ jobs/pgx, jobs/mysql) | — | River,
asynq, machinery, gocraft/work |`, with depguard rules (message prefix `D7`)
that deny those libraries.

## 11. Out of scope

Cron expressions, a web UI, job dependencies or workflows, cancelling a job
from outside by ID (can be added later as `Client.Cancel(ctx, id)`), and
Redis or other non-SQL backends.
