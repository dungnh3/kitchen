# migrate

`migrate` applies versioned SQL (and Go) migrations to PostgreSQL or MySQL.
It is a thin layer over goose v3: each migration runs in its own
transaction, so a failed migration rolls back and leaves no "dirty" state.
Concurrent runs are serialised by a lock. The `kmigrate` CLI uses the same
code path. It is the canonical migrations package (D7 row "Migrations" in
[`docs/decisions.md`](../docs/decisions.md)): do not use goose or
golang-migrate directly.

## Install

```sh
go get github.com/dungnh3/kitchen/migrate
go install github.com/dungnh3/kitchen/migrate/cmd/kmigrate@latest
```

`migrate` is its own submodule. It depends on the core module for `log`,
`pg` and `mysqlx`.

## Quick start

Write one file per migration, named `<version>_<name>.sql`. The version is
any increasing number; `kmigrate create` uses a UTC timestamp.

```sql
-- 20250304001122_initdb.sql
-- +goose Up
CREATE TABLE items (id bigserial PRIMARY KEY, name text NOT NULL);

-- +goose Down
DROP TABLE items;
```

Embed the directory and apply everything pending:

```go
//go:embed testdata/pg/*.sql
var migrations embed.FS
```

```go
ctx := context.Background()
pool, err := pg.Open(ctx, pg.Config{URL: os.Getenv("DATABASE_URL")})
if err != nil {
	return
}
defer pool.Close()

logger := log.Default() // the process logger main installed with log.Setup
m, err := migrate.NewPostgres(pool, migrate.Config{Source: migrations, Dir: "testdata/pg", Logger: logger})
if err != nil {
	return
}
defer func() { _ = m.Close() }() // does not close pool

if err := m.Up(ctx); err != nil {
	logger.ErrorContext(ctx, "migrations failed; the failed one was rolled back", log.Err(err))
}
```

In a service, use `//go:embed sql/*.sql` and `Dir: "sql"`. `Dir` is the
directory inside the `fs.FS`; the default is `"."`.

What the defaults give you:

- **One transaction per migration.** A failure rolls back that migration.
  Earlier ones stay applied. There is no `Force` and no dirty flag.
- **A lock.** Pods that start together wait up to one minute for the lock.
  Only one of them applies anything.
- **Nothing pending is not an error.** `Up` on a current database returns nil.
- **Your connection stays yours.** `Close` never closes a pool or `*sql.DB`
  passed to `NewPostgres` or `NewMySQL`.
- **Context everywhere.** Cancelling `ctx` aborts the running migration's
  transaction.
- **No global state.** goose's global registry is disabled.

### File format

- `-- +goose Up` and `-- +goose Down` start the two sections. Down is
  optional, but without it you cannot roll back.
- Do not write `BEGIN`/`COMMIT`: goose wraps each file in a transaction.
- `-- +goose NO TRANSACTION` at the top runs the file outside a transaction.
  Use it only for statements that refuse one, such as
  `CREATE INDEX CONCURRENTLY`. Keep such files to one statement: a failure
  in the middle is not rolled back.
- Wrap bodies with semicolons inside, such as `$$` functions, in
  `-- +goose StatementBegin` and `-- +goose StatementEnd`.
- MySQL commits DDL implicitly. A failed MySQL DDL migration is **not**
  rolled back, so keep one DDL statement per file on MySQL.

## Configuration

`migrate.Config`, and the matching `kmigrate` flag:

| Field | kmigrate flag | Default | When to change it |
|---|---|---|---|
| `Source` | `-path DIR` | none (required unless `GoMigrations`) | Always: your `embed.FS` (`kmigrate` reads the directory) |
| `Dir` | (part of `-path`) | `"."` | The directory inside `Source` |
| `GoMigrations` | none | none | Data changes written in Go |
| `Table` | `-table` | `goose_db_version` | Several apps or modules share one database |
| `LockTimeout` | `-lock-timeout` | `1m` | A migration runs longer than the wait |
| `AllowOutOfOrder` | `-allow-out-of-order` | `false` | Branches merge migrations with interleaved versions |
| `Logger` | `LOG_*` env vars | `log.Default()` | Pass the logger `main` built |

Connection settings:

| Constructor | kmigrate | Connection |
|---|---|---|
| `NewPostgres(pool, cfg)` | | Your `*pgxpool.Pool` from `pg.Open` |
| `NewMySQL(db, cfg)` | | Your `*sql.DB` from `mysqlx.Open` |
| `Open(ctx, dialect, dsn, cfg)` | `-dialect postgres\|mysql`, `-database DSN` (default `$DATABASE_URL`) | Opened with `pg.Open`/`mysqlx.Open`, 2 connections, closed by `Close` |

`Open` and `kmigrate` use the same TLS defaults as services: TLS is required
unless the DSN turns it off (`sslmode=disable`, `tls=false`). Turn it off
only for a local database. In production it protects the credentials and
the schema in transit.

## Integration with kitchen

### Logger

Pass the logger from `log.Setup` as `Config.Logger`. Each applied migration
logs `migrate: applied` with `direction`, `version`, `path` and `duration`.
goose's own messages go to the same logger.

### Run at service startup

Migrate after opening the pool and before the server starts serving. Then
the new code never sees the old schema. Full service wiring is in
[`../server/USAGE.md`](../server/USAGE.md).

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

opts, err := log.OptionsFromEnv()
logger, _ := log.Setup(opts)
if err != nil {
	logger.WarnContext(ctx, "invalid log config", log.Err(err))
}

pool, err := pg.Open(ctx, pg.Config{URL: os.Getenv("DATABASE_URL")}, pg.WithLogger(logger))
if err != nil {
	logger.ErrorContext(ctx, "postgres", log.Err(err))
	return
}
defer pool.Close()

// Several pods may start at once: they wait on the migration lock, and
// only the first one applies anything.
m, err := migrate.NewPostgres(pool, migrate.Config{Source: migrations, Dir: "testdata/pg", Logger: logger})
if err != nil {
	logger.ErrorContext(ctx, "migrate", log.Err(err))
	return
}
err = m.Up(ctx)
_ = m.Close() // releases the migrator; pool stays open for the service
if err != nil {
	logger.ErrorContext(ctx, "migrate", log.Err(err))
	return
}

srv, err := server.New(
	server.WithServices( /* your services, built on pool */ ),
	server.WithAdmin(":9090"),
)
```

Use this pattern when the service owns its schema and the database user may
run DDL. A failed migration stops the pod before it takes traffic.

### MySQL

The same flow with `mysqlx.Open` and `NewMySQL`:

```go
ctx := context.Background()
logger := log.Default()
db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: os.Getenv("MYSQL_DSN")}, mysqlx.WithLogger(logger))
if err != nil {
	return
}
defer func() { _ = db.Close() }()

m, err := migrate.NewMySQL(db, migrate.Config{Source: mysqlMigrations, Dir: "testdata/mysql", Logger: logger})
if err != nil {
	return
}
defer func() { _ = m.Close() }() // does not close db

if err := m.Up(ctx); err != nil {
	logger.ErrorContext(ctx, "migrate", log.Err(err))
}
```

### Readiness while a job migrates

When a separate job applies migrations (see [kmigrate](#the-kmigrate-cli)),
the service should not take traffic until the job is done. `HasPending`
takes no lock, so it never waits behind a running migration. Use it as a
readiness check:

```go
migrated := server.Check{Name: "migrations", Func: func(ctx context.Context) error {
	pending, err := m.HasPending(ctx)
	if err != nil {
		return err
	}
	if pending {
		return errors.New("migrations pending")
	}
	return nil
}}
srv, err := server.New(
	server.WithServices( /* your services */ ),
	server.WithAdmin(":9090", server.WithAdminChecks(migrated)),
)
```

The server caches check results for one second, so probes do not flood the
database.

### Errors

Errors are plain wrapped errors: `migrate: up: ...`. They are startup or job
errors, not request errors, so they do not go through `errs`. Log them and
exit non-zero. `errors.Is(err, context.Canceled)` works when the context was
cancelled.

## Advanced

### The kmigrate CLI

Run migrations as a separate step: a Kubernetes Job, an init container, or a
CI deploy stage. Use it when the service user must not run DDL, when
migrations are slow, or when you deploy many replicas.

```sh
kmigrate -database "$DATABASE_URL" -path ./sql up
kmigrate -dialect mysql -database "$MYSQL_DSN" -path ./sql up
```

Commands:

| Command | Effect |
|---|---|
| `up` | Apply all pending migrations |
| `down N` | Revert exactly `N` migrations, newest first |
| `to VERSION` | Migrate up or down to `VERSION` (`0` reverts everything) |
| `version` | Print the newest applied version (`0` when none) |
| `status` | Print `state`, applied time and file name per migration |
| `create NAME` | Write `<UTC timestamp>_NAME.sql` with an Up/Down template into `-path`; needs no database |

`NAME` must match `[a-z0-9_]+`. Flags go before the command:

```sh
kmigrate -path ./sql create add_price
kmigrate -database "$DATABASE_URL" -path ./sql -table billing_version -lock-timeout 5m status
kmigrate -database "$DATABASE_URL" -path ./sql down 1
```

Command output (`version`, `status`, `create`) goes to stdout. Logs go to
stderr as text, configured by `LOG_LEVEL`, `LOG_FORMAT`, `LOG_SCHEMA` and
`LOG_BACKEND`; `LOG_FORMAT=json` gives JSON. Set `LOG_OUTPUT=stdout` only
when nothing parses the command output: logs then mix with it.
SIGINT and SIGTERM cancel the context and roll back the running migration.

In a Kubernetes init container or Job, give `kmigrate` the same image tag as
the service, so the files match the code. Pair it with the readiness check
above.

### Checking for pending migrations in CI

Fail a deploy stage when the database is behind the code:

```go
ctx := context.Background()
logger := log.Default()
m, err := migrate.Open(ctx, migrate.Postgres, os.Getenv("DATABASE_URL"),
	migrate.Config{Source: migrations, Dir: "testdata/pg", Logger: logger})
if err != nil {
	logger.ErrorContext(ctx, "migrate", log.Err(err))
	return
}
defer func() { _ = m.Close() }() // closes the connection Open made

pending, err := m.HasPending(ctx) // takes no lock
if err != nil {
	logger.ErrorContext(ctx, "migrate", log.Err(err))
	return
}
if pending {
	logger.ErrorContext(ctx, "database has pending migrations")
}
```

`kmigrate status` shows the same in a shell. `HasPending` returns an error
when an older version is missing and `AllowOutOfOrder` is off.

### Rolling back

```go
_ = m.Down(ctx, 1)            // revert the newest migration
_ = m.To(ctx, 20250304100000) // up or down to exactly this version
v, _ := m.Version(ctx)        // 20250304100000
_ = m.To(ctx, 0)              // revert everything
```

`Down(ctx, n)` needs `n >= 1` and stops without error when nothing is left.
`Status` lists each migration with `State` (`pending` or `applied`) and
`AppliedAt`.

### Go migrations

Use a Go migration for a data change that SQL cannot express. SQL and Go
migrations run together in version order:

```go
up := func(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "UPDATE items SET name = lower(name)")
	return err
}
m, err := migrate.NewPostgres(pool, migrate.Config{
	Source:       migrations,
	Dir:          "testdata/pg",
	GoMigrations: []*migrate.GoMigration{migrate.NewGoMigration(20250320000000, up, nil)},
})
```

A nil function records the version without running anything.
`NewGoMigration` runs inside the migration's transaction.
`NewGoMigrationNoTx` runs without one, for statements such as
`CREATE INDEX CONCURRENTLY`; a failure there leaves the database partially
migrated. Neither needs a goose import, which D7 forbids outside `migrate`.

### Locking, shared databases and merged branches

- **Postgres** uses a session advisory lock. It needs a real session: run
  migrations on a direct connection, not through PgBouncer in transaction
  mode.
- **MySQL** uses a lock table, `goose_lock`.
- Both wait up to `LockTimeout` and then fail. Raise it when one migration
  takes longer than a minute, or other pods will give up.
- `Table` keeps separate histories when several modules share one database.
- `AllowOutOfOrder` applies an older version that was merged after a newer
  one was deployed. Without it, `Up` fails and names the missing version.

```go
cfg := migrate.Config{
	Source:          migrations,
	Dir:             "testdata/pg",
	Table:           "billing_schema_version", // one history per module sharing a database
	LockTimeout:     5 * time.Minute,          // a long migration holds the lock
	AllowOutOfOrder: true,                     // apply older versions merged late
	Logger:          log.Default(),
}
```

## Testing

- Run migrations against a real database. The package tests use
  testcontainers (`postgres:18-alpine`, `mysql:8.4`); they skip with
  `-short` or `KITCHEN_SKIP_DOCKER=1`.
- Test Down sections too: `Up`, then `To(ctx, 0)`, then `Up` again.
- Use `log.Discard()` as `Config.Logger` to keep test output quiet.
- Give each parallel test its own schema (`search_path=...` in the URL) or
  its own `Table`.

## Observability

- Log record `migrate: applied` per migration, with `direction` (`up` or
  `down`), `version`, `path` and `duration`.
- goose's own messages go through the same logger.
- Queries run on your pool or `*sql.DB`, so the spans and metrics from `pg`
  and `mysqlx` cover them. `migrate` adds no metrics of its own.

## Production checklist

- Embed migrations in the binary; never read them from a mutable volume.
- Pick one pattern per service: startup migration or a `kmigrate` job plus
  the `HasPending` readiness check.
- Keep TLS on. Use `sslmode=disable` or `tls=false` only locally.
- Postgres: migrate on a direct connection, not PgBouncer transaction mode.
- Set `LockTimeout` above your slowest migration.
- One non-transactional statement per `NO TRANSACTION` file; one DDL
  statement per file on MySQL.
- Make migrations backward compatible with the running version during a
  rolling deploy: add columns first, drop them a release later.

## Migrating from the old kitchen

The old `migrations` package used golang-migrate. The API map and the
one-time file and history conversion are in
[`docs/migration/data-utils.md`](../docs/migration/data-utils.md#one-time-conversion-from-golang-migrate).
