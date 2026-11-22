# mongox

`mongox` opens an instrumented MongoDB client on the official driver v2.
`Open` returns a plain `*mongo.Client`, so you use the driver API directly.
On top of the driver, mongox adds safe defaults: TLS, short timeouts and a
bounded idle pool. It also adds OTel spans and metrics, and maps driver
errors to `errs`. It is the canonical MongoDB package (D7 row "MongoDB" in
[`docs/decisions.md`](../docs/decisions.md)): do not call `mongo.Connect`
yourself.

## Install

```sh
go get github.com/dungnh3/kitchen/mongox
```

`mongox` is its own submodule. It depends on the core module
(`github.com/dungnh3/kitchen`) for `log`, `errs` and `otelx`.

## Quick start

```go
ctx := context.Background()
client, err := mongox.Open(ctx, mongox.Config{
	URI:     os.Getenv("MONGO_URI"), // mongodb://user:pass@db-0:27017,db-1:27017/?replicaSet=rs0
	AppName: "orders",
})
if err != nil {
	return
}
defer func() { _ = client.Disconnect(context.WithoutCancel(ctx)) }()

var doc bson.M
err = client.Database("app").Collection("orders").
	FindOne(ctx, bson.D{{Key: "_id", Value: "o1"}}).Decode(&doc)
if errors.Is(mongox.MapError(err), errs.ErrNotFound) {
	return // no such order
}
```

`Open` builds the driver options, installs the OTel monitors, connects and
pings the **primary**. If the ping fails, it disconnects and returns the
error, so a failed `Open` never leaves a client connected. You own the
client: call `Disconnect` on shutdown.

What you get by default:

- **TLS on remote hosts.** TLS 1.2 or newer is required, and the server
  certificate is verified. See [TLS](#tls).
- **Fail fast.** Connect and server selection time out after 5s, not the
  driver's 30s. A service with a wrong URI fails at start-up.
- **Bounded idle pool.** Idle connections close after 5 minutes. The driver
  default keeps them forever.
- **Telemetry.** Every command gets a span and a duration metric. Pool
  connections are counted. Command bodies are never recorded.
- **Logging.** One `mongox: connected` record with the hosts and the TLS
  state, through `log.Default()` unless you pass a logger.

## Configuration

`Config` is a plain struct. Zero values use the defaults below.

| Field | Default | When to change it |
|---|---|---|
| `URI` | required | `mongodb://` or `mongodb+srv://`. URI options (replica set, auth source, `tls`, read preference) work as in the driver. |
| `AppName` | none | Always set it. The server shows it in logs and `currentOp`. |
| `MaxPoolSize` | 100 per server | Raise it for many concurrent requests per instance. Watch `db.client.connection.count{state=used}`. |
| `MinPoolSize` | 0 | Keep warm connections for latency-sensitive paths. Must not exceed `MaxPoolSize`. |
| `MaxConnIdleTime` | 5m | Lower it behind proxies or firewalls that drop idle TCP. |
| `ConnectTimeout` | 5s | Raise it only for slow cross-region links. |
| `ServerSelectionTimeout` | 5s | Also bounds the start-up ping. Raise it if the cluster often fails over at deploy time. |
| `Timeout` | 0 (none) | Client-side operation timeout, used only when the call's `ctx` has no deadline. |

There are no environment variables. Read `MONGO_URI` (or your own name) in
`main` and fill the struct.

The same config with every default written out:

```go
client, err := mongox.Open(ctx, mongox.Config{
	URI:                    os.Getenv("MONGO_URI"),
	AppName:                "orders",
	MaxPoolSize:            100,              // per server (driver default)
	MinPoolSize:            0,                // keep no warm connections
	MaxConnIdleTime:        5 * time.Minute,  // close idle connections
	ConnectTimeout:         5 * time.Second,  // TCP + TLS handshake
	ServerSelectionTimeout: 5 * time.Second,  // also bounds the ping in Open
	Timeout:                10 * time.Second, // only when ctx has no deadline; 0 = none
})
```

`Open` also takes options:

| Option | Default |
|---|---|
| `WithLogger(*log.Logger)` | `log.Default()` |
| `WithTracerProvider(trace.TracerProvider)` | the global provider |
| `WithMeterProvider(metric.MeterProvider)` | the global provider |

### TLS

mongox turns TLS on unless one of these is true:

- the URI chooses: it has `tls=` or `ssl=` (true or false);
- the URI is `mongodb+srv://`, which implies TLS in the driver;
- every host is loopback (`localhost`, `127.0.0.1`, `::1`).

The certificate is verified against the system roots. A local
`mongodb://localhost:27017` works with no TLS and no extra setting.

A plaintext server on a non-loopback host, such as `mongo:27017` in Docker
Compose, fails the handshake. Add `?tls=false` to that URI only.

> **Security warning.** `tls=false` sends credentials and data in clear
> text. `tlsInsecure=true` or `tlsAllowInvalidCertificates=true` accept any
> certificate, so anyone on the network path can impersonate the server.
> The default exists because cloud networks are shared. Use these settings
> only for local containers, never in a production URI.

## Integration with kitchen

### Logger, readiness and the server

`main` calls `log.Setup` once and passes the logger in. The client's `Ping`
is a `server.Check` for `/readyz`:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

opts, _ := log.OptionsFromEnv()
logger, _ := log.Setup(opts)

client, err := mongox.Open(ctx, mongox.Config{URI: os.Getenv("MONGO_URI"), AppName: "orders"},
	mongox.WithLogger(logger))
if err != nil {
	logger.ErrorContext(ctx, "mongo", log.Err(err))
	return
}
defer func() { _ = client.Disconnect(context.WithoutCancel(ctx)) }()

srv, err := server.New(
	server.WithLogger(logger),
	server.WithServices( /* your services, holding client */ ),
	server.WithReadiness(server.Check{
		Name: "mongo",
		Func: func(ctx context.Context) error { return client.Ping(ctx, nil) },
	}),
)
if err != nil {
	logger.ErrorContext(ctx, "server", log.Err(err))
	return
}
if err := srv.Run(ctx); err != nil {
	logger.ErrorContext(ctx, "server", log.Err(err))
}
```

The server runs checks with a 5s timeout and caches the result for one
second, so probes do not flood the database. `Ping(ctx, nil)` uses the
client's read preference. A worker without a gRPC server uses
`server.NewAdmin` with `server.WithAdminChecks` instead. Full service wiring
is in [`../server/USAGE.md`](../server/USAGE.md).

### Errors

Wrap every driver error with `MapError` before you return it. The result is
an `errs` sentinel with the driver error as its cause, so logs keep the
details. Clients only see the sentinel's generic message, so key values and
index names never leak.

```go
err := mongox.MapError(fmt.Errorf("get order: %w", mongo.ErrNoDocuments))
fmt.Println(errors.Is(err, errs.ErrNotFound), errs.Code(err))

dup := mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000, Message: "E11000 duplicate key"}}}
err = mongox.MapError(dup)
fmt.Println(errors.Is(err, errs.ErrAlreadyExists), errs.Code(err))
// Output:
// true NotFound
// true AlreadyExists
```

| Driver error | errs sentinel |
|---|---|
| `mongo.ErrNoDocuments` | `ErrNotFound` |
| duplicate key (11000, 11001, 12582) | `ErrAlreadyExists` |
| 121 document validation failed | `ErrFailedPrecondition` |
| 112 write conflict, `TransientTransactionError` label | `ErrAborted` (retry the transaction) |
| 50 `MaxTimeMSExpired`, client-side timeout | `ErrDeadlineExceeded` |
| network errors, not primary, shutdown, server selection, client disconnected | `ErrUnavailable` |

`nil`, errors that are already `*errs.Error`, `context.Canceled` and
`context.DeadlineExceeded` come back unchanged; `errs` classifies context
errors itself. Errors with no meaningful mapping also come back unchanged
and become `Internal` at the transport. A failed `Open` maps to
`ErrUnavailable`.

### Telemetry

mongox records through the global OTel providers. `otelx/sdk.Setup`
installs them in `main`, so you usually pass nothing. Use
`WithTracerProvider` and `WithMeterProvider` only to send this client
somewhere else, or in tests. The spans are children of the span in the
call's `ctx`, so a request traced by `server` shows its Mongo commands.

## Advanced

### Driver options that Config does not cover

Use `ClientOptions` to build the options, change them, then call `Connect`.
`Connect` still installs the OTel bridge and pings the primary. Use it for
read preference, compression, write concern, a custom TLS config, or
credentials from a secret store.

```go
ctx := context.Background()
co, err := mongox.ClientOptions(mongox.Config{URI: os.Getenv("MONGO_URI"), AppName: "reports"})
if err != nil {
	return
}
co.SetReadPreference(readpref.SecondaryPreferred())
co.SetCompressors([]string{"zstd", "snappy"})

client, err := mongox.Connect(ctx, co)
if err != nil {
	return
}
defer func() { _ = client.Disconnect(context.WithoutCancel(ctx)) }()
```

`ClientOptions` applies the TLS rule, so a `SetTLSConfig` call afterwards
replaces kitchen's TLS config. Keep `MinVersion` at TLS 1.2 or newer and
never set `InsecureSkipVerify`.

### Your own monitors

The driver takes one `CommandMonitor` and one `PoolMonitor`. `Connect`
installs its own only where `co.Monitor` or `co.PoolMonitor` is nil. A
monitor you set first **replaces** that half of the bridge. There is no hook
to chain yours with kitchen's.

- Your `PoolMonitor`: spans and `db.client.operation.duration` stay;
  `db.client.connection.count` stops.
- Your `CommandMonitor`: the pool metric stays; command spans and the
  duration metric stop.

```go
ctx := context.Background()
logger := log.Default()
co, err := mongox.ClientOptions(mongox.Config{URI: os.Getenv("MONGO_URI")})
if err != nil {
	return
}
co.SetPoolMonitor(&event.PoolMonitor{Event: func(e *event.PoolEvent) {
	if e.Type == event.ConnectionPoolCleared {
		logger.Warn("mongo pool cleared", "address", e.Address)
	}
}})

client, err := mongox.Connect(ctx, co, mongox.WithLogger(logger))
if err != nil {
	return
}
defer func() { _ = client.Disconnect(context.WithoutCancel(ctx)) }()
```

If you replace the command monitor, never log or record command bodies.
They hold user data.

### Transactions

Use the driver's `session.WithTransaction`. It already retries on
`TransientTransactionError`. If you run transactions by hand, retry when
`errors.Is(mongox.MapError(err), errs.ErrAborted)`.

## Testing

- **Unit tests** need no server. `ClientOptions` connects to nothing, so you
  can assert on the built options. `MapError` is pure: feed it
  `mongo.WriteException` or `mongo.CommandError` values.
- **Integration tests** use testcontainers (`mongo:8`). Skip them under
  `-short` or when Docker is unavailable. See `TestOpenIntegration` in
  `mongox_test.go`.
- **Telemetry assertions.** Pass an in-memory tracer and meter provider with
  `WithTracerProvider` and `WithMeterProvider`. Tests may use the OTel SDK
  directly (D7). Assert on span names such as `insert users`.
- **Logs.** Pass `WithLogger(log.Discard())`.
- **Leaks.** Use `goleak.VerifyTestMain` and `Disconnect` every client.

## Observability

Spans are named `{command} {collection}`, for example `find users`. A
command without a collection uses `{command} {database}`. Kind is client.

| Span attribute | Value |
|---|---|
| `db.system.name` | `mongodb` |
| `db.operation.name` | the command name |
| `db.namespace` | the database |
| `db.collection.name` | the collection, when the command has one |
| `server.address`, `server.port` | the server that ran the command |
| `error.type` | on failure: the server code name, `timeout`, `network` or `_OTHER` |
| `db.response.status_code` | on failure: the server error code |

Failed commands record the error through `otelx.RecordError`. Command
bodies (`db.query.text`) are **never** recorded.

| Metric | Attributes |
|---|---|
| `db.client.operation.duration` (s) | the span attributes above, without the span kind |
| `db.client.connection.count` | `db.client.connection.state` = `idle` or `used`; `db.client.connection.pool.name` = the server address |

Logs: one `mongox: connected` record at info level with `hosts` and `tls`.
Errors: see the table in [Errors](#errors).

This bridge exists because driver v2 has no official OTel integration yet.
It will be replaced by the official one when it ships; the names follow
the semantic conventions, so dashboards should keep working.

## Production checklist

- Set `AppName`.
- Keep TLS on. Never ship `tls=false`, `tlsInsecure=true` or
  `tlsAllowInvalidCertificates=true` in a production URI.
- Use a replica set or `mongodb+srv` URI, so `Open` can find the primary.
- Give every request a deadline, or set `Timeout`.
- Wrap every driver error with `MapError` before it leaves your repository.
- Add the `mongo` readiness check, and `Disconnect` on shutdown.
- Size `MaxPoolSize` from `db.client.connection.count`, and keep instances ×
  pool size within the server's connection limit.

## Migrating from the old kitchen

mongox is new; the old kitchen had no MongoDB package. Moving from a
hand-written driver v1 client is covered in
[`docs/migration/data-utils.md`](../docs/migration/data-utils.md).
