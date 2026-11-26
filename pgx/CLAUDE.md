# pgx: package rules

`pgx` is the canonical Postgres entry point (D7), built on jackc/pgx v5.
Database code shares the same defaults across `pgx`, `mysqlx` and
`mongox`; keep them aligned.

- **Two packages named pgx.** Any file that imports both kitchen `pgx` and
  `github.com/jackc/pgx/v5` imports jackc as `jpgx`, in code and in docs.
  Inside this package, jackc is always `jpgx`.

- **TLS is required by default** (`SSLMode` "require"). Never fall back to
  plaintext unless the caller sets "disable" explicitly.
- **A URL is the whole config.** Setting `Host`, `Port` or `SSLMode` next to
  a URL is an error, so pgx's fallbacks and TLS ServerName cannot drift from
  the URL host. Range-check ports.
- **Errors:** `MapError` turns jackc/pgx and Postgres errors into `errs`
  sentinels. The cause stays in logs only; SQL text and values never reach
  clients.
- **Telemetry and readiness:**
  - Tracing and metrics come from otelpgx plus pool-statistics callbacks.
  - Open one pool per database per process; the stats callbacks are never
    unregistered (see `WithoutPoolStats`).
  - Readiness is `pool.Ping`. There is no `pgx.Ping`.
- **The package sets no time zone.** Services set
  `RuntimeParams["timezone"]` themselves.
- **gorm:** `pgx/gorm` wraps the same pool and logs through `log/gormlog`.
  Never open a second pool for gorm.
- **Tests that need a real server** read `KITCHEN_TEST_PG_DSN` and skip
  without it.
