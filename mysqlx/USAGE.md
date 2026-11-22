# mysqlx

`mysqlx` opens an instrumented `*sql.DB` for MySQL, with safe defaults. Use
it for every MySQL connection, whether you query with `database/sql`, sqlc or
gorm (through `mysqlx/gorm`). It is the canonical MySQL package
([D7](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22)):
do not call `sql.Open` or the gorm MySQL driver directly.

## Install

```sh
go get github.com/dungnh3/kitchen            # mysqlx (core module)
go get github.com/dungnh3/kitchen/mysqlx/gorm # only if you use gorm
```

## Quick start

```go
ctx := context.Background()
db, err := mysqlx.Open(ctx, mysqlx.Config{
	DSN:          os.Getenv("MYSQL_DSN"), // user:pass@tcp(host:3306)/db
	MaxOpenConns: 50,
})
if err != nil {
	return
}
defer func() { _ = db.Close() }()

_ = server.Check{Name: "mysql", Func: db.PingContext} // readiness
var n int
_ = db.QueryRowContext(ctx, "SELECT count(*) FROM orders").Scan(&n)
```

`Open` connects and pings; on error nothing is left open. You close the
`*sql.DB`.

What you get by default:

- **TLS required** (`TLSRequire`): encrypted, but the certificate is not
  verified. See [TLS](#tls) before production.
- **Times in UTC**: `DATETIME` values are read as `time.Time` in UTC.
- **Unsafe driver options off**: multi statements, client-side interpolation
  and `LOAD DATA LOCAL` of any file stay off unless your DSN turns them on.
- **Pool sized for a service**: 25 open and 25 idle connections, 30m
  lifetime, 5m idle time. `database/sql` alone keeps only 2 idle.
- **Tracing and metrics** through otelsql, without SQL text.

## Configuration

Give either `DSN`, or `Addr` plus `Database`. With `DSN`, only `User`,
`Password` and `Database` may override it. Put the address and TLS inside the
DSN; setting `Addr` or `TLSMode` next to a DSN is an error.

| Field | Default | When to change |
|---|---|---|
| `DSN` | — | You already have a go-sql-driver DSN. No `tls` param means `TLSRequire`. |
| `Addr` | — | Structured config. `host` or `host:port`; port defaults to 3306. |
| `User`, `Password`, `Database` | — | Credentials and schema; override the DSN when set. |
| `TLSMode` | `TLSRequire` | `TLSVerify` or a registered config in production. Not with `DSN`. |
| `MaxOpenConns` | 25 | Raise for high concurrency; keep the sum over replicas below `max_connections`. |
| `MaxIdleConns` | `MaxOpenConns` | Lower only if idle connections cost you. |
| `ConnMaxLifetime` | 30m | Keep below the server's `wait_timeout` and any proxy idle timeout. |
| `ConnMaxIdleTime` | 5m | Lower to release connections faster after a burst. |
| `ConnectTimeout` | 5s (or the DSN's `timeout`) | Slow networks. Also bounds the ping in `Open`. |
| `ReadTimeout`, `WriteTimeout` | none | Per-packet I/O timeouts. Prefer context deadlines per query. |
| `Loc` | UTC | Only if the database stores local time in `DATETIME`. |
| `Params` | none | Session variables sent on connect, e.g. `sql_mode`. |

The same config without a DSN, with full TLS verification:

```go
ctx := context.Background()
db, err := mysqlx.Open(ctx, mysqlx.Config{
	Addr:     "mysql.internal", // port defaults to 3306
	User:     "orders",
	Password: os.Getenv("MYSQL_PASSWORD"),
	Database: "orders",
	TLSMode:  mysqlx.TLSVerify, // verify the certificate and host name
	Params:   map[string]string{"sql_mode": "'STRICT_ALL_TABLES'"},
})
if err != nil {
	return
}
defer func() { _ = db.Close() }()
```

### TLS

| Mode | Value | What it does |
|---|---|---|
| `TLSRequire` (default) | `skip-verify` | Encrypts, does not verify the certificate. Like Postgres `sslmode=require`. |
| `TLSVerify` | `true` | Encrypts and verifies the certificate and host name against the system roots. |
| `TLSDisable` | `false` | Plaintext, including the password exchange. Local servers only. |
| any other name | — | A `*tls.Config` you registered with `mysql.RegisterTLSConfig`. Prefer `Config.TLS`. |

`TLSRequire` is the default because MySQL 8 ships a self-signed certificate,
so verification would fail on a fresh server. It stops passive sniffing, but
**not an active man in the middle**. In production use `TLSVerify`, or
register your provider's CA (see [Custom CA](#custom-ca)). A typo in
`TLSMode` fails in `ParseConfig`, not on the first query.

`TLSDisable` exists for a local docker-compose server without TLS. Never use
it for a server reached over a network: anyone on the path can read the
password and every row.

## Integration with kitchen

### Logger, server and readiness

`mysqlx` logs one line on connect (address, database, TLS mode). Pass the
process logger from `log.Setup` with `WithLogger`; nil means `log.Default()`.
Register `db.PingContext` as a readiness check with `server.WithReadiness`.
The server caches the result for one second and bounds it with a 5s timeout.

```go
ctx := context.Background()
opts, _ := log.OptionsFromEnv()
logger, _ := log.Setup(opts) // once, in main

db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: os.Getenv("MYSQL_DSN")},
	mysqlx.WithLogger(logger))
if err != nil {
	logger.ErrorContext(ctx, "mysql", log.Err(err))
	return
}
defer func() { _ = db.Close() }()

srv, err := server.New(
	server.WithServices( /* your services, holding db */ ),
	server.WithReadiness(server.Check{Name: "mysql", Func: db.PingContext}),
)
if err != nil {
	logger.ErrorContext(ctx, "server", log.Err(err))
	return
}
_ = srv.Run(ctx)
```

Full service wiring (errors, auth, admin port, shutdown) is in
[server/USAGE.md](../server/USAGE.md).

### Errors

`MapError` turns `database/sql` and MySQL errors into `errs` sentinels. The
server renders them as gRPC and HTTP statuses. The MySQL message (key, table
and column names) stays in the cause, for logs only.

```go
dup := fmt.Errorf("insert user: %w", &mysql.MySQLError{
	Number: 1062, Message: "Duplicate entry 'a@b.c' for key 'users.email'",
})
err := mysqlx.MapError(dup)
fmt.Println(errors.Is(err, errs.ErrAlreadyExists), errs.Code(err))

err = mysqlx.MapError(fmt.Errorf("get user: %w", sql.ErrNoRows))
fmt.Println(errors.Is(err, errs.ErrNotFound), errs.HTTPStatus(err))
// Output:
// true AlreadyExists
// true 404
```

The main mappings: duplicate key → `ErrAlreadyExists`; foreign key, null and
check violations → `ErrFailedPrecondition`; bad values → `ErrInvalidArgument`;
deadlock and lock wait timeout → `ErrAborted`; `max_execution_time` →
`ErrDeadlineExceeded`; too many connections, read-only and bad connections →
`ErrUnavailable`. The `MapError` godoc has the full list of error numbers.
Other errors come back unchanged; the server turns unknown ones into INTERNAL.

### Telemetry

`mysqlx` uses the global OTel providers, so call `otelx/sdk.Setup` in `main`
before `Open` (or pass `WithTracerProvider` / `WithMeterProvider`). Query
spans nest under your `otelx.Start` spans when you pass the context along.

### sqlc and gorm

sqlc's generated `New(db)` takes the `*sql.DB` as is. For gorm, wrap the same
`*sql.DB` with [`mysqlx/gorm`](#mysqlxgorm-package-mysqlgorm).

## Advanced

### Deadlocks and retries

InnoDB aborts one side of a deadlock. Retry the whole transaction on
`errs.ErrAborted`:

```go
ctx := context.Background()
var db *sql.DB // from mysqlx.Open
transfer := func(ctx context.Context) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return mysqlx.MapError(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - 10 WHERE id = ?", 1); err != nil {
		return mysqlx.MapError(err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + 10 WHERE id = ?", 2); err != nil {
		return mysqlx.MapError(err)
	}
	return mysqlx.MapError(tx.Commit())
}
for range 3 {
	// Deadlock (1213) and lock wait timeout (1205) map to ErrAborted.
	if err := transfer(ctx); !errors.Is(err, errs.ErrAborted) {
		break
	}
}
```

### Custom CA

Managed databases (RDS, Cloud SQL, Azure) sign with their own CA. Put it in
`Config.TLS`: the certificate and the host name are verified, and no driver
import is needed. `Config.TLS` cannot be combined with `TLSMode` or a DSN
`tls` parameter.

```go
pem, err := os.ReadFile("/etc/mysql/ca.pem") // your provider's CA bundle
if err != nil {
	return
}
roots := x509.NewCertPool()
roots.AppendCertsFromPEM(pem)
db, err := mysqlx.Open(context.Background(), mysqlx.Config{
	Addr: "mysql.internal", User: "orders", Database: "orders",
	// Verifies the certificate against roots and the host name
	// (ServerName defaults to the Addr host).
	TLS: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
})
if err != nil {
	return
}
defer func() { _ = db.Close() }()
```

### Short-lived passwords and driver hooks

`ParseConfig` builds the driver config without connecting. Change it, then
call `OpenConfig`. Use this for IAM tokens or Vault leases fetched per
connection, or a custom dialer.

```go
ctx := context.Background()
mc, err := mysqlx.ParseConfig(mysqlx.Config{Addr: "mysql.internal", User: "orders", Database: "orders"})
if err != nil {
	return
}
// Fetch a short-lived password (cloud IAM token, Vault lease) per connection.
err = mc.Apply(mysql.BeforeConnect(func(_ context.Context, c *mysql.Config) error {
	c.Passwd = os.Getenv("MYSQL_TOKEN") // replace with your token source
	return nil
}))
if err != nil {
	return
}
db, err := mysqlx.OpenConfig(ctx, mc)
if err != nil {
	return
}
defer func() { _ = db.Close() }()
// OpenConfig keeps database/sql's pool defaults: set the limits yourself.
db.SetMaxOpenConns(25)
db.SetMaxIdleConns(25)
db.SetConnMaxLifetime(10 * time.Minute) // below the token lifetime
```

### Many short-lived pools

Pool statistics are registered as MeterProvider callbacks that are never
unregistered. Open one `*sql.DB` per database per process. If you open and
close pools at runtime (per tenant, in tests), pass `WithoutPoolStats`, or
give those pools a dedicated MeterProvider:

```go
// Per-tenant pools that are opened and closed at runtime: skip the
// pool-stats callbacks, which would otherwise never be unregistered.
openTenant := func(ctx context.Context, dsn string) (*sql.DB, error) {
	return mysqlx.Open(ctx, mysqlx.Config{DSN: dsn, MaxOpenConns: 5}, mysqlx.WithoutPoolStats())
}
db, err := openTenant(context.Background(), os.Getenv("TENANT_DSN"))
if err != nil {
	return
}
defer func() { _ = db.Close() }()
```

## Backends and adapters

### `mysqlx/gorm` (package `mysqlgorm`)

`mysqlgorm.Open` wraps an existing `*sql.DB` from `mysqlx`. gorm then uses
that pool and its otelsql tracing, so you need no gorm tracing plugin. You
still own the `*sql.DB`: close it once, not through gorm.

```go
ctx := context.Background()
db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: os.Getenv("MYSQL_DSN")})
if err != nil {
	return
}
defer func() { _ = db.Close() }() // close the *sql.DB once; gorm does not own it

gdb, err := mysqlgorm.Open(db, mysqlgorm.Options{})
if err != nil {
	return
}
var u User
err = mysqlgorm.MapError(gdb.WithContext(ctx).First(&u, "email = ?", "a@b.c").Error)
if errors.Is(err, errs.ErrNotFound) {
	return // the handler returns err: NOT_FOUND on gRPC, 404 on REST
}

// sqlc (or plain database/sql) on the same pool and tracing:
//   q := store.New(db)
var n int
_ = db.QueryRowContext(ctx, "SELECT count(*) FROM users").Scan(&n)
```

Logging goes through `log/gormlog`. By default it logs errors and queries
slower than 200ms to `log.Default()`, without parameter values, and skips
`ErrRecordNotFound`. A custom `Log` replaces those defaults as a whole:

```go
ctx := context.Background()
logger := log.Default() // the process logger, set up by log.Setup in main

db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: os.Getenv("MYSQL_DSN")}, mysqlx.WithLogger(logger))
if err != nil {
	return
}
defer func() { _ = db.Close() }()

gdb, err := mysqlgorm.Open(db, mysqlgorm.Options{
	Log: &gormlog.Config{
		Logger:               logger,
		SlowThreshold:        100 * time.Millisecond,
		IgnoreRecordNotFound: true, // a Log replaces the defaults: keep this set
	},
	// Leave TranslateError off: it hides the MySQL error MapError reads.
	Config: &gorm.Config{SkipDefaultTransaction: true},
})
if err != nil {
	return
}
_ = gdb
```

- Map every gorm error with `mysqlgorm.MapError`: `mysqlx.MapError` plus
  `gorm.ErrRecordNotFound` → `errs.ErrNotFound`.
- Do not set `gorm.Config.TranslateError`: it replaces the MySQL error with
  `gorm.ErrDuplicatedKey` and friends, so the mapping is lost.
- `gormlog.Config.LogParams` writes parameter values into logs. Keep it off
  outside local debugging: values often hold personal data.

## Testing

- `ParseConfig` and `MapError` need no server. For errors, build a
  `*mysql.MySQLError{Number: 1062}` and assert with `errors.Is`.
- For real queries, the package's own tests use testcontainers
  (`mysql:8.4`). They skip with `-short` or `KITCHEN_SKIP_DOCKER=1`.
- Tests that open many databases pass `WithoutPoolStats` and
  `WithLogger(log.Discard())`, and close every `*sql.DB` (goleak checks).

## Observability

- **Spans** (otelsql): `sql.connector.connect`, `sql.conn.query`,
  `sql.conn.exec`, `sql.conn.begin_tx`, `sql.tx.commit`, and so on. They carry
  `db.system.name=mysql`, `db.namespace`, `server.address` and `server.port`.
  SQL text, rows and session-reset spans are not recorded: literals may hold
  personal data.
- **Metrics**: `db.client.operation.duration` and pool statistics
  (`db.sql.connection.*`: open, wait, wait_duration, closed_max_*).
- **Logs**: one INFO line `mysqlx: connected` with `addr`, `database` and
  `tls`. gorm logs through `log/gormlog` with `db.query.text` and `elapsed`.

## Production checklist

- Use `TLSVerify` or a registered CA. Never `TLSDisable` off localhost.
- Keep the DSN and password in a secret store, not in code or images.
- Keep `MaxOpenConns` × replicas below `max_connections`, and
  `ConnMaxLifetime` below `wait_timeout`.
- Do not add `multiStatements=true`, `interpolateParams=true` or
  `allowAllFiles=true` to the DSN.
- Register `db.PingContext` with `server.WithReadiness`; return
  `MapError(err)` from every data-access path.
- Open one `*sql.DB` per database per process.

## Migrating from the old kitchen

`mysqlx` is new: the old kitchen had no MySQL package, only a MySQL config
struct in `storage`. To move from raw go-sql-driver or gorm code, see
[docs/migration/data-utils.md](../docs/migration/data-utils.md).
