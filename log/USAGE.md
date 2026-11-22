# log

Package `log` is kitchen's only logging package. Every kitchen package logs
through it, so format, schema and backend are chosen in one place. It writes
structured JSON for your platform (OpenTelemetry-style, Google Cloud Logging
or AWS CloudWatch) and correlates records with the active trace. It is the
canonical package for logging: see D5 and D7 in
[docs/decisions.md](../docs/decisions.md#d7-canonical-packages-owner-2026-11-22).

`*log.Logger` is an alias of `*slog.Logger`. Hand it to any library that
takes a `*slog.Logger` (OpenTelemetry, pgx, gorm, goose, the MCP SDK)
without conversion.

## Install

```sh
go get github.com/dungnh3/kitchen            # package log
go get github.com/dungnh3/kitchen/log/gormlog # gorm logger (optional)
```

## Quick start

Call `log.Setup` once, early in `main`:

```go
opts, err := log.OptionsFromEnv()
logger, _ := log.Setup(opts)
if err != nil {
	logger.Warn("invalid logging environment", log.Err(err))
}
logger.Info("service starting", log.String("version", "v1.2.3"))
```

`Setup` installs the process logger. After it:

- `log.Default()` writes through it. So do loggers taken from `Default`
  before the call, and loggers derived from them with `With`.
- Every kitchen package whose `Logger` option is nil uses it.
- `log/slog`'s default logger and the standard `log` package write through
  it, so third-party code lands in the same stream.

The defaults:

- JSON, one object per line, on stdout.
- Level INFO, with the source location (`file:line`).
- Schema detected from the platform (Cloud Run, Lambda, ECS, ...), else
  SchemaOTel.
- Records at ERROR and above carry a stack trace.
- `trace_id`/`span_id` on every record logged with a `*Context` method
  while a span is active.
- `request_id` and `user_id` on every record logged with a request context
  (set by `kitchen/context`).
- User attributes cannot spoof built-in keys. A user `message` key is
  written as `attr.message`.

Without `Setup`, `log.Default()` still writes kitchen JSON built from the
environment, never Go's text handler. Libraries never call `Setup`.

## Configuration

`OptionsFromEnv` reads these variables. An invalid value is returned as an
error, and the returned `Options` still hold usable defaults.

| Env var | Values | Default | Change it when |
|---|---|---|---|
| `LOG_LEVEL` | debug, info, notice, warn, error, critical, alert, emergency | info | debugging; `opts.Level` is a `*slog.LevelVar`, so you can change it at runtime |
| `LOG_FORMAT` | json, text | json | local development (text keeps slog's key names) |
| `LOG_SCHEMA` | otel, gcp, aws | detected | on GKE or EKS, which detection cannot see |
| `LOG_BACKEND` | zap, slog | zap | you want no zap in the encoding path |
| `LOG_OUTPUT` | stdout, stderr | stdout | stdout is a data channel (MCP over stdio, CLIs) |

`Options` fields that have no env var:

| Field | Default | Use |
|---|---|---|
| `NoSource` | false | drop the source location |
| `StackTraceLevel` / `NoStackTrace` | ERROR / false | move or disable stack traces |
| `ContextAttrs` | nil | derive attributes from a context (must be cheap) |
| `ReplaceAttr` | nil | rewrite or redact user attributes |
| `Handlers` | nil | extra sinks, such as the OTLP log bridge |
| `Output` | stdout | where `Setup` writes |

The schemas differ in their key names:

| Schema | Level key | Trace keys | Stack trace key |
|---|---|---|---|
| `SchemaOTel` | `level` (WARN) | `trace_id`, `span_id`, `trace_flags` | `exception.stacktrace` |
| `SchemaGCP` | `severity` (WARNING) | `logging.googleapis.com/trace`, `.../spanId`, `.../trace_sampled` | `stack_trace` (Error Reporting) |
| `SchemaAWS` | `level` (WARN) | `trace_id`, `span_id`, `xray_trace_id` | `stack_trace` |

## Integration with kitchen

**Packages take a `*log.Logger`.** Every kitchen package has a `Logger`
option, or a `WithLogger` option, and resolves nil with `log.OrDefault`.
Write your own packages the same way:

```go
type workerConfig struct {
	// Logger receives the worker's records. nil means log.Default().
	Logger *log.Logger
}

func newWorker(cfg workerConfig) *worker { return &worker{log: log.OrDefault(cfg.Logger)} }

func (w *worker) run(ctx context.Context, jobID string) {
	// Scope a value to everything logged with ctx.
	ctx = log.WithAttrs(ctx, log.String("job_id", jobID))
	w.log.InfoContext(ctx, "job started", log.Int("attempt", 1))
}
```

Build attributes with `log.String`, `log.Int`, `log.Duration`, `log.Err`,
`log.Group` and the other helpers. Use the `log.Level*` constants for
levels. Never import `log/slog` outside `log/`: depguard and forbidigo fail
the build (D5).

**Request context.** `kitchen/server` stores `request_id` for every request,
and `auth` adds `user_id` through `xcontext.WithUserID`. Log with the request
context and both appear on the record:

```go
ctx := xcontext.NewContext(context.Background(), xcontext.Info{RequestID: "r-1"})
ctx = xcontext.WithUserID(ctx, "u-42")
logger.InfoContext(ctx, "order placed", log.String("order_id", "o1"))
// {"message":"order placed","level":"INFO","order_id":"o1","request_id":"r-1","user_id":"u-42"}
```

**Errors.** `log.Err(err)` writes `"error": err.Error()`, and nothing when
`err` is nil. The `server` access log and the errs interceptor already log
failed requests, so do not log an error and also return it.

**Traces.** Use the `*Context` methods (`InfoContext`, `ErrorContext`, ...).
The trace keys come from the span in the context. Logging never creates span
events and never sets span status. Record errors on spans with `otelx`.

**OTLP logs.** Ship records to an OpenTelemetry collector as well as stdout
with `Options.Handlers`. Add `tel.SlogHandler("app")` from `otelx/sdk`, as
shown under [Advanced](#extra-sinks).

**Server.** `server.New` uses `log.Default()` unless you pass
`server.WithLogger`. See [server/USAGE.md](../server/USAGE.md) for full
service wiring.

## Advanced

### Scoped attributes in workers

`log.WithAttrs` stores attributes in a context. A new value for an existing
key replaces the old one. Use it for job ids, tenants and batch ids:

```go
ctx := log.WithAttrs(context.Background(), log.String("tenant", "acme"))
ctx = log.WithAttrs(ctx, log.String("tenant", "globex")) // same key: replaced
logger.InfoContext(ctx, "invoice sent", log.Group("invoice", log.String("id", "inv-1"), log.Float64("total", 12.5)))
// {"message":"invoice sent","level":"INFO","invoice":{"id":"inv-1","total":12.5},"tenant":"globex"}
```

Context attributes stay at the top level, even when the logger has groups.

### Programs whose stdout is data

An MCP server over stdio, `kmigrate`, or any CLI that prints results must
keep stdout clean:

```go
opts, err := log.OptionsFromEnv()
if opts.Output == nil {
	opts.Output = os.Stderr
}
logger, _ := log.Setup(opts)
if err != nil {
	logger.Warn("invalid logging environment", log.Err(err))
}
```

Also set `LOG_OUTPUT=stderr` in the deployment. That covers records logged
before `Setup` runs.

### Extra sinks

`Options.Handlers` adds outputs. It never replaces the kitchen JSON. Each sink
gets every record at or above `Level`, with the context attributes added.
A sink writes its own format.

```go
var audit bytes.Buffer
sink := log.New(&audit, log.Options{Backend: log.BackendSlog, Format: log.FormatText}).Handler()

opts, _ := log.OptionsFromEnv()
opts.Handlers = append(opts.Handlers, sink)
logger, _ := log.Setup(opts)
logger.Info("service starting")
```

With `otelx/sdk`, the sink is `tel.SlogHandler("app")`.

### APIs that want a standard `*log.Logger`

```go
srv := &http.Server{
	ReadHeaderTimeout: 10 * time.Second,
	ErrorLog:          log.StdLogger(logger, log.LevelWarn),
}
srv.ErrorLog.Print("http: TLS handshake error")
// {"message":"http: TLS handshake error","level":"WARN"}
```

### Explicit schema and backend

`OptionsFromEnv` detects the schema on Cloud Run, Cloud Functions, App
Engine, Lambda and ECS. On GKE or EKS, set `LOG_SCHEMA`, or pick the schema in
code:

```go
logger := log.New(os.Stdout, log.Options{Schema: log.SchemaGCP, NoSource: true})
logger.Warn("quota low", log.String("message", "user text cannot spoof the message"))
// {"message":"quota low","severity":"WARNING","attr.message":"user text cannot spoof the message"}
```

A schema for another platform is a `log.Schema` struct literal. zap is the
default encoder. `Backend: log.BackendSlog` uses slog's own handlers and
produces the same JSON. `log.NewZap` returns a `*zap.Logger` for hot paths
that prefer zap's API. It has no context, so it has no trace correlation.

## Backends and adapters

### log/gormlog

`gormlog` implements gorm's `logger.Interface` on top of `*log.Logger`.

```go
// Records go to log.Default() (package kitchen/log), the process logger
// main installs with log.Setup; set Config.Logger to use another one.
l := gormlog.New(gormlog.Config{
	SlowThreshold:        500 * time.Millisecond,
	IgnoreRecordNotFound: true,
})
// gorm.Open(dialector, &gorm.Config{Logger: l})
```

The defaults:

- Errors and slow queries (over 200ms) are logged.
- SQL is logged with placeholders. Parameter values are never logged.
- The source location points at your code, not at gorm.

`pg/gorm` and `mysqlx/gorm` already install it, with
`IgnoreRecordNotFound: true`. Override it with their `Options.Log`. They
trace queries through the pool, so you need no gorm tracing plugin.

For local debugging only, you can log every query with its values:

```go
// Local debugging: every query at INFO, with parameter values. Never
// enable LogParams in production: parameters often hold personal data
// or secrets.
l := gormlog.New(gormlog.Config{
	Logger:    log.Default(),
	Level:     logger.Info,
	LogParams: true,
})
```

## Testing

Use `log.Discard()` when a test does not look at logs:

```go
w := newWorker(workerConfig{Logger: log.Discard()})
w.run(context.Background(), "j-1")
```

To assert on logs, write JSON to a buffer:

```go
var buf bytes.Buffer
w := newWorker(workerConfig{Logger: log.New(&buf, log.Options{NoSource: true})})
w.run(context.Background(), "j-2")
fmt.Println(strings.Contains(buf.String(), `"job_id":"j-2"`))
// true
```

Pass the logger in through the package's `Logger` option. Do not call
`log.Setup` or `log.SetDefault` in tests: they change process-wide state,
which leaks into tests that run in parallel.

## Observability

- Built-in keys per schema are listed under [Configuration](#configuration).
- `request_id` and `user_id` come from `kitchen/context`. Other context keys
  come from `log.WithAttrs` and `Options.ContextAttrs`.
- `error` comes from `log.Err`.
- Stack traces start at the caller, in `runtime/debug.Stack` format.
- `attr.<key>` holds a user attribute that tried to use a reserved key.

## Production checklist

- Call `log.Setup(opts)` once in `main`, before anything else logs.
- Check the error from `OptionsFromEnv`. A typo in `LOG_LEVEL` falls back
  to INFO.
- Set `LOG_SCHEMA` on GKE and EKS.
- Set `LOG_OUTPUT=stderr` for stdio MCP servers and CLIs.
- Log with `*Context` methods so trace ids and request ids are attached.
- Keep messages constant and put data in attributes (sloglint `static-msg`).
- Never log credentials or raw request bodies. Keep `gormlog.LogParams` off.

## Migrating from the old kitchen

The old `l` and `log` packages map to this package. Key names, levels and
schemas are listed in
[docs/migration/observability.md](../docs/migration/observability.md).
