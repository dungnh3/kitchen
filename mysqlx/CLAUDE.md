# mysqlx: package rules

`mysqlx` is the canonical entry point for MySQL (D7). Keep its defaults in
line with `pgx` and `mongox`.

## Safe defaults

- TLS is required by default (`TLSRequire`: encrypted, not verified, like
  `sslmode=require`).
- Times are parsed into `time.Time` in UTC.
- The driver's unsafe options stay off unless the DSN turns them on
  explicitly:
  - multi statements;
  - client-side interpolation;
  - `LOAD DATA LOCAL` of any file.

  Never enable any of them by default.

## Errors

`MapError` maps MySQL errors to `errs` sentinels. The cause is kept for logs
only.

## Telemetry

- Tracing and metrics come from otelsql.
- SQL text is never recorded.
- Open one `*sql.DB` per database per process. The pool-stats callbacks are
  never unregistered (see `WithoutPoolStats`).
- For readiness, use `db.PingContext`.

## gorm

`mysqlx/gorm` wraps the same `*sql.DB` and logs through `log/gormlog`.
