# Migration: jobs

Design and rationale: [docs/design/jobs.md](../design/jobs.md). kitchen had no
background job queue before `jobs`, so there is no symbol map. This page
records the wire contracts the package introduces and how to move periodic
work that runs on `lease.Elector`.

## Contracts

These are stored in the database or sent on the wire; changing one needs a
migration.

| Contract | Value |
|---|---|
| Table | one per driver (`WithTable`, default `jobs`), columns as in `Driver.Schema()` |
| States | `available`, `running`, `completed`, `cancelled`, `discarded` |
| Job kind | `Args.Kind()`, at most 255 bytes of `[A-Za-z0-9_.:/-]`; stored in `kind` |
| Args | `encoding/json` of the args struct, in `args` |
| Trace context | the propagator's text map as a JSON object, in `metadata` |
| Error record | `{"at": RFC 3339 UTC, "attempt": n, "error": "..."}`, the latest 10 in `errors` |
| Periodic unique key | `periodic:<kind>:<slot start, Unix seconds>` |
| PostgreSQL wake-up | `NOTIFY` on channel `<table>_jobs` (`.` in the table name becomes `_`) |
| Log keys | `job_id`, `job_kind`, `queue`, `attempt`, `max_attempts` |
| Metrics | `kitchen.jobs.inserted`, `.finished`, `.duration`, `.queue_lag`, attributes `kitchen.jobs.kind`, `.queue`, `.outcome` |

## From `lease.Elector` jobs

A periodic function that runs under `lease.Elector` (the generated
`internal/job` package) becomes a typed job with a schedule:

| Before | After |
|---|---|
| a `job.Job` with an interval and a function, run by the elected replica | `jobs.Every(interval, newArgs)` and a `jobs.Worker` registered in the registry |
| a failed run is lost | a failed run is retried with backoff and kept as a row |
| runs only while the replica holds the lease | any replica runs it; one row per slot has a unique key |
| needs Redis (`lease/redis`) | needs the service database |

Differences to plan for:

- The interval is aligned to the Unix epoch (`24*time.Hour` runs at 00:00 UTC),
  not to the start of the process.
- A slot that passes while every replica is down is skipped, as a missed
  tick was before.
- Each slot is its own job, so a run that is slower than the interval can
  overlap the next slot's run, on any replica; keep the worker idempotent and
  safe to run concurrently.

`-with lease` and `zen add job` still exist for services that have no SQL
database.
