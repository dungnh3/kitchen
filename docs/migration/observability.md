# Migration: `l`, `log`, `tracing` → `log`, `otelx`

This guide covers moving from the old packages `l`, `log` and `tracing` to the new ones. The design is in [docs/design/observability.md](../design/observability.md).

Below, `$OLD` means the old library's module path. Set it in your shell before running the recipes:

```bash
export OLD=<old module path>
```

New import paths:

| Package | Import path | Notes |
|---|---|---|
| `log` | `github.com/dungnh3/kitchen/log` | core module |
| `otelx` | `github.com/dungnh3/kitchen/otelx` | core module |
| `sdk` | `github.com/dungnh3/kitchen/otelx/sdk` | separate module; only `main` imports it |
| `gcp` | `github.com/dungnh3/kitchen/otelx/gcp` | separate module |
| `cloudtrace` | `github.com/dungnh3/kitchen/otelx/gcp/cloudtrace` | separate module, deprecated |
| `prometheus` | `github.com/dungnh3/kitchen/otelx/prometheus` | separate module |
| `gormlog` | `github.com/dungnh3/kitchen/log/gormlog` | separate module |

## 1. Logging: everything goes through `kitchen/log`

Both `l` and `log` collapse into `kitchen/log` (owner decision 2026-11-22, docs/decisions.md D5). Packages take a `*log.Logger` (an alias of `*slog.Logger`, so it still fits libraries that take one), default to `log.Default()`, and build attributes with `log.String`, `log.Err`, ...; only `kitchen/log` imports `log/slog`. `main` calls `log.Setup` once:

```go
opts, err := log.OptionsFromEnv()
logger, _ := log.Setup(opts) // log.Default(), slog's default and the stdlib log package
if err != nil {
	logger.Warn("invalid logging environment", log.Err(err))
}
```

Programs whose stdout is a data channel (MCP over stdio, CLIs) set `opts.Output = os.Stderr` before `log.Setup`, or `LOG_OUTPUT=stderr` so that logs written before Setup go to stderr too.

Request id and user id need no wiring: `kitchen/context` adds them to the context's log attributes (`xcontext.LogAttrs` and `Options{ContextAttrs: xcontext.LogAttrs}` are gone).

### Constructors and types

| Old | New |
|---|---|
| `l.New()`, `l.NewWithName(n)` | `log.New(os.Stdout, opts)`; for a name: `.With(slog.String("logger", n))` |
| `log.NewLogger(cfg)`, `log.MustNewLogger(cfg)`, `log.NewDefaultLogger()` | `opts, err := log.OptionsFromEnv(); logger, _ := log.Setup(opts)` in main; elsewhere `log.Default()` |
| `l.Logger`, `*l.Logger`, `log.LoggerInterface`, `log.Logger` | `*log.Logger` (kitchen/log, alias of `*slog.Logger`) |
| `log.Config{Level, Encoder}` | `log.Options{Level, Format}` |
| `log.Config.Backend` (`zap`/`gcp`/`hybrid`), `ProjectID`, `LogName` | removed: always JSON on stdout, in the `Options.Schema` format (`SchemaGCP` for Cloud Logging). |
| `Config.Options []zap.Option`, `Development`, `Sampling*` | removed |
| `logger.SetLevel(l)` | pass a `*slog.LevelVar` as `Options.Level` and call `lv.Set(l)`; lowering now works too |
| env `LOG_ENCODER` (`json`/`console`/`gcp`) | env `LOG_FORMAT` (`json`/`text`), read only by `log.OptionsFromEnv` |
| env `LOG_BACKEND`, `GCP_PROJECT_ID`, `GCP_LOG_NAME`, `LOG_DEVELOPMENT` | removed; `LOG_SCHEMA=gcp|aws|otel` (or detection) picks the format |
| `logger.Named(n)` | `logger.With(slog.String("logger", n))` |
| `logger.S.Infof(...)` (sugar) | `logger.Info(fmt.Sprintf(...))`, or better, structured attrs |
| `log.NewRestyLogger` | removed; use the HTTP client's own hooks with `*slog.Logger`, or instrument with `otelx.HTTPTransport` |
| `log.NewGormLogger(opts...)` | `gormlog.New(gormlog.Config{Logger, Level, SlowThreshold, IgnoreRecordNotFound})` |
| `Log(ctx, level, msg, ...any)` (go-grpc-middleware adapter) | provided by the transport area's interceptor package, not `log` |

### Methods

| Old | New |
|---|---|
| `x.Debug/Info/Warn/Error(msg, fields...)` | unchanged shape: `x.Info(msg, attrs...)` |
| `x.WithContext(ctx).Info(msg, f...)`, `x.Ctx(ctx).Info(...)` | `x.InfoContext(ctx, msg, attrs...)` |
| `x.InfoWithCtx` / `WarnWithCtx` / `ErrorWithCtx(ctx, msg, f...)` | `x.InfoContext` / `WarnContext` / `ErrorContext(ctx, msg, attrs...)` |
| `x.DPanic(msg)` | `x.Log(ctx, log.LevelCritical, msg)` |
| `x.Panic(msg)` | `x.Log(ctx, log.LevelAlert, msg); panic(msg)` |
| `x.Fatal(msg)` | `x.Log(ctx, log.LevelEmergency, msg); os.Exit(1)`, in `main` only |
| `x.With(fields...)` (returned `*zap.Logger`) | `x.With(attrs...)` returns `*slog.Logger` |

### Field helpers

| Old (`l.` or `log.`) | New |
|---|---|
| `String`, `Int`, `Int64`, `Uint64`, `Bool`, `Float64`, `Duration`, `Time`, `Any` | `slog.String`, `slog.Int`, … (same names) |
| `Int8/16/32(k, v)` | `slog.Int(k, int(v))` |
| `Uint/Uint32(k, v)` | `slog.Uint64(k, uint64(v))` |
| `Float32(k, v)` | `slog.Float64(k, float64(v))` |
| `Object`, `Interface`, `Strings`, `Ints`, `Bools`, `Binary`, `ByteString` | `slog.Any(k, v)` |
| `Stringer(k, v)` | `slog.String(k, v.String())` |
| `Error(err)` | `log.Err(err)` (key `error`, value `err.Error()`, dropped when nil) |
| `Namespace(k)` | `logger.WithGroup(k)` |
| `Skip()` | `slog.Attr{}` |
| `Stack()`, `StackSkip(n)` | removed; for crashes use `otelx.WithSpan`, which records panics with a stack trace |
| `TraceID(id)`, `SpanID(id)` | automatic from ctx; remove them |

### Request metadata

The old `log` read a fixed allow-list of gRPC metadata headers on every log call. The new design splits the work:
- `Options.ContextAttrs` reads request-scoped values (request id, user id from `kitchen/context`) for every record; `log.WithAttrs` is for ad-hoc values;
- every `*Context` log call made with that context includes those attributes.

### Codemod recipe

This recipe assumes the old import used the alias `l`. That holds for 1086 of the old `log` imports and for every `l` import. Run it per repository and review the diff.

Do not use `gofmt -r` for this. It treats every single-letter identifier, `l` included, as a wildcard, so `attribute.String(k, v)` would be rewritten too. The recipe uses `perl`, which behaves the same with GNU and BSD tools.

1. Check that no local variable is named `l`. If one is, its `l.Error("msg")` calls would be rewritten.

   ```bash
   files=$(grep -rlE "\"$OLD/(l|log)\"" --include='*.go' .)
   grep -nE '\bl :?= |var l\b|func\s*\(l ' $files
   ```

2. Field helpers:

   ```bash
   perl -pi -e '
     s/\bl\.(String|Int|Int64|Uint64|Bool|Float64|Duration|Time|Any)\(/slog.$1(/g;
     s/\bl\.(Object|Interface|Strings|Ints|Bools|Binary|ByteString)\(/slog.Any(/g;
     s/\bl\.Error\(/klog.Err(/g;
   ' $files
   ```

   `Int8/16/32`, `Uint/Uint32`, `Float32` and `Stringer` need a conversion (see the table above). They are left unchanged and show up as compile errors to fix by hand.

3. Context methods. This works when the ctx argument contains no parentheses:

   ```bash
   perl -pi -e '
     s/\.(?:WithContext|Ctx)\(([^()]*)\)\.(Debug|Info|Warn|Error)\(/.${2}Context($1, /g;
     s/\.(Info|Warn|Error)WithCtx\(/.${1}Context(/g;
   ' $files
   ```

4. Types and imports:

   ```bash
   perl -pi -e 's/\*?\bl\.(?:Logger|LoggerInterface)\b/*slog.Logger/g' $files
   perl -pi -e "s#(?:\\bl\\s+)?\"\Q$OLD\E/(?:l|log)\"#klog \"github.com/dungnh3/kitchen/log\"#" $files
   goimports -w $files   # adds "log/slog", drops unused klog
   ```

5. Constructors (`l.New`, `log.NewDefaultLogger`, package-level `var logger = ...`) need manual edits. Build one logger in `main` and pass it down (D5). Then run `go build ./... && go vet ./...`.

## 2. Tracing → `otelx` + `otelx/sdk`

### Setup

| Old | New |
|---|---|
| `tracing.Init(ctx, log, svc, &InitConfig{...})` → `TracingHandle` | `sdk.Setup(ctx, sdk.Config{...})` → `(*sdk.Telemetry, error)` |
| `tracing.Register(log, svc, opts...)`, `RegisterTracer(log, cfg, svc)` | `sdk.Setup` |
| `handle.Shutdown(ctx)`, `tp.Shutdown(ctx)` | `tel.Shutdown(ctx)` (idempotent; restores globals) |
| `ForceFlush` | `tel.ForceFlush(ctx)` |
| `GlobalProvider()`, `handle.TracerProvider` | `tel.TracerProvider()` or `otel.GetTracerProvider()` |
| `UpdateSamplingRate`, `HealthCheck`, `GetConfig` | removed (the first two never did anything) |
| `WithCodeAttributes()` | removed |

`InitConfig` fields map as follows:

| `InitConfig` | New |
|---|---|
| `Enabled: false` | `sdk.Config{Disabled: true}` or `OTEL_SDK_DISABLED=true` |
| `Environment` | `Config.Environment` (now `deployment.environment.name`) |
| `TagVersion` | `Config.ServiceVersion` |
| `SamplingRate *float64` | `Config.SampleRatio *float64`, or `OTEL_TRACES_SAMPLER_ARG` |
| `ExcludeOperations` | `Config.ExcludeOperations` (added to `sdk.DefaultExcludedOperations`) |
| `ExportTimeout` | `Config.Traces.Timeout`, or `OTEL_EXPORTER_OTLP_TIMEOUT` |
| `MaxQueueSize` | `OTEL_BSP_MAX_QUEUE_SIZE` |
| `Provider: "tempo"/"otlp"`, `Endpoint`, `OTLPProtocol` | `Config.Traces{Endpoint, Protocol, Insecure}`. The default protocol is now **grpc**: set `Protocol: sdk.ProtocolHTTPProtobuf` to keep `http`. |
| `Provider: "gcp"` (OTLP) | `exp, _ := gcp.TraceExporter(ctx, gcp.Config{ProjectID, CredentialsJSON, CredentialsFile})`, then `Config.SpanExporters: {exp}`, `ResourceDetectors: {gcp.ResourceDetector(gcpCfg)}` |
| `GCPExporter: "cloudtrace"` | `creds, _ := gcp.Credentials(ctx, c)`; `p, _ := gcp.ProjectID(ctx, c, creds)`; `cloudtrace.New(ctx, cloudtrace.Config{ProjectID: p, Credentials: creds})`. Deprecated; move to `gcp.TraceExporter`. |
| `CredentialsJSON string` | `gcp.Config.CredentialsJSON []byte` |
| `DisableResourceDetection` | `gcp.Config.DisableDetection` |
| `Provider: "coralogix"`, `APIKey`, `Endpoint` | see the Coralogix recipe below |

Coralogix is plain OTLP. The old Coralogix sampler wrapper is gone.

```go
sdk.Config{
    ServiceName: "orders",
    Traces: sdk.SignalConfig{
        Endpoint: "ingress.<region>.coralogix.com:443",
        Headers:  map[string]string{"Authorization": "Bearer " + apiKey},
    },
    ResourceAttrs: []attribute.KeyValue{
        attribute.String("cx.application.name", app),
        attribute.String("cx.subsystem.name", "orders"),
    },
}
```

The same works with environment variables only:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=https://ingress.<region>.coralogix.com:443
OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer <key>"
OTEL_RESOURCE_ATTRIBUTES="cx.application.name=<app>,cx.subsystem.name=orders"
```

### Span helpers

| Old | New |
|---|---|
| `tracing.StartSpan(ctx, name, opts...)` | `otelx.Start(ctx, name, opts...)` |
| `StartSpanWithService(ctx, svc, name)` | `otelx.Start(ctx, name)`. `service.name` belongs on the resource, not the span. |
| `RecordError(span, err, description)` | `otelx.RecordError(span, err)`. The status description is now `err.Error()`. The `error.type`/`error.message`/`error.description` attrs are gone; the exception event carries `exception.type`/`exception.message`. |
| per-repo `EndSpan` wrappers | `defer otelx.End(span, &err)` |
| `RecordPanic(span, r)` | `otelx.WithSpan` records and re-raises panics; elsewhere use `otelx.RecordError(span, fmt.Errorf("panic: %v", r))` |
| `WithSpan(ctx, name, func(ctx, span) error)` | `otelx.WithSpan(ctx, name, func(ctx) error)`; get the span with `trace.SpanFromContext(ctx)` |
| `WithSpanAsync`, `MeasureLatency`, `AddBusinessLogic`, `AddUserInfo` | removed; use `span.SetAttributes` / `span.AddEvent` directly |
| `CreateEvent(span, name, attrs...)` | `span.AddEvent(name, trace.WithAttributes(attrs...))` |
| `AddHTTPInfo` | use `otelx.HTTPHandler`/`HTTPTransport` (otelhttp adds current semconv), or `semconv.HTTPRequestMethodKey`, `semconv.URLFull`, `semconv.HTTPResponseStatusCode` |
| `AddDatabaseInfo` | `semconv.DBNamespace`, `semconv.DBQueryText` |
| `TraceIDFromContext`, `SpanIDFromContext` | `otelx.TraceID(ctx)`, `otelx.SpanID(ctx)` |
| `SpanContextFromContext` | `trace.SpanContextFromContext` |
| `CloudTraceName(ctx, project)` | removed; `SchemaGCP` writes the bare trace id (Cloud Logging's preferred form) |

### Propagation

| Old | New |
|---|---|
| `MapCarrier` | `propagation.MapCarrier` |
| `InjectToCarrier(ctx, c)` / `ExtractFromCarrier(ctx, c)` | `otelx.Inject(ctx, c)` / `otelx.Extract(ctx, c)` |
| `InjectHTTPHeaders(ctx, h)` | `otelx.Inject(ctx, propagation.HeaderCarrier(h))` |
| `ExtractFromHTTPRequest(r)` | `otelx.Extract(r.Context(), propagation.HeaderCarrier(r.Header))` |

### gRPC/HTTP instrumentation

| Old | New |
|---|---|
| `NewDefaultStatsHandler`, `NewStatsHandlerWithConfig`, `NewProductionStatsHandler`, `NewDevelopmentStatsHandler`, `NewCustomStatsHandler`, `CreateBasicStatsHandler`, `CreateEnhancedStatsHandler` | `otelx.GRPCServerHandler(otelgrpc options...)` |
| `NewClientHandler`, `ClientDialOptions` | `grpc.WithStatsHandler(otelx.GRPCClientHandler())` |
| `StatsConfig.SkipMetrics` | removed (it never did anything); metrics follow the MeterProvider |
| per-repo `HTTPTransport` wrappers | `otelx.HTTPTransport(base)` |
| sampler `NewOperationFilterSampler` | `sdk.NewSampler(ratio, exclude)`; the sampler is built in by default |
| OTel error handler (stdlib `log` by default) | `sdk.Config.Logger` (`*log.Logger`, nil → `log.Default()`) receives export errors and, with globals installed, OTel's internal logr diagnostics |

### GORM

| Old | New |
|---|---|
| `GormTracingPlugin`, `NewGormTracingPlugin`, `GormDatabaseWrapper`, `*WithTracing` | Postgres: `pgxgorm.Open(pool, ...)` on a `pgx.Open` pool (otelpgx traces it). Other drivers: `db.Use(tracing.NewPlugin())` from `gorm.io/plugin/opentelemetry/tracing` |
| `log.NewGormLogger(...)` | `gormlog.New(gormlog.Config{...})` |

### Metrics (new)

```go
reader, _ := prometheus.NewReader(prometheus.Options{}) // keeps /metrics via DefaultRegisterer
tel, _ := sdk.Setup(ctx, sdk.Config{MetricReaders: []sdkmetric.Reader{reader}})
```

OTLP metrics: set `Config.Metrics.Exporter = sdk.ExporterOTLP`, or `OTEL_METRICS_EXPORTER=otlp`.

### OTLP logs (new, opt-in)

```go
tel, _ := sdk.Setup(ctx, sdk.Config{Logs: sdk.SignalConfig{Exporter: sdk.ExporterOTLP}})
opts, _ := log.OptionsFromEnv()
opts.Handlers = append(opts.Handlers, tel.SlogHandler("orders")) // stdout JSON + OTLP
logger, _ := log.Setup(opts)
```

## 3. Behaviour changes

### Output schema

| Area | Old | New |
|---|---|---|
| Time key | `l`: `time` (ISO8601); `log`: `timestamp` (RFC3339) | `time` (RFC3339Nano) |
| Message key | `msg` | `message` |
| Severity | `l`: `WARN`; `log`: `WARNING`; JSON encoder: lowercase | always the Cloud Logging enum: `DEBUG` `INFO` `NOTICE` `WARNING` `ERROR` `CRITICAL` `ALERT` `EMERGENCY` |
| Trace keys | `trace_id`, `span_id`, `trace_sampled` | `logging.googleapis.com/trace`, `logging.googleapis.com/spanId`, `logging.googleapis.com/trace_sampled`. Traces now link in Cloud Logging. |
| Caller | `caller`, `func`/`function` | `logging.googleapis.com/sourceLocation{file,line,function}` |
| Stack | `stacktrace`/`stack_trace` on errors | none |
| Error | `l`: string; `log`: `error` + `errorVerbose` | `error` (string only) |
| Logger name | `logger` | only if you add `slog.String("logger", n)` |
| gRPC metadata | added automatically from a fixed allow-list | what `Options.ContextAttrs` returns (wired from `kitchen/context`) |

Update any log-based metrics, alerts and saved queries that use the old keys.

### Defaults

| Area | Old | New |
|---|---|---|
| Level (repos that used `l`) | `DEBUG` | `INFO`; set `LOG_LEVEL=debug` if needed |
| Env vars | read implicitly by constructors | `log.New` never reads env; call `log.OptionsFromEnv()` in `main` |
| Invalid `LOG_LEVEL` | panic (`l`) or ignored (`log`) | an error from `OptionsFromEnv` |
| OTLP protocol for the otlp/tempo provider | `http` | `grpc` |
| `Setup` errors | `Init` swallowed every error and returned no-op tracing | `Setup` returns the error; decide in `main` whether to fail |
| GORM logger level | Info: every query at INFO | Warn: errors and slow queries |
| GORM slow threshold | 2s, logged at INFO | 200ms, logged at WARN |
| GORM SQL | interpolated with values | placeholders only (`LogParams: true` to opt in) |
| GORM keys | `sql`, `rows`, `elapsed` | `db.query.text`, `db.response.returned_rows`, `elapsed` |
| Sampler exclusions | Pub/Sub `StreamingPull` | also `grpc.health.v1.Health/` and `grpc.reflection.` |
| Health checks in gRPC traces | depended on the preset | skipped by `otelx.GRPCServerHandler` |
| `deployment.environment` | old attribute | `deployment.environment.name` (semconv v1.43.0) |

### Old bugs fixed (no longer reproducible)

`l`:
1. Every log through a `Ctx(ctx)` logger called `span.End()`, cutting spans short at the first log line. Logging no longer touches spans.
2. `Ctx(ctx).S` was nil. The sugar API is gone, and `*Context` methods are used instead.
3. The gRPC `Log()` adapter ignored ctx, so interceptor logs had no trace. Context is always honoured.
4. `Error(err)` dropped the error type into a plain string, and `fmt.Errorf` was called with a non-constant format. Both are gone.
5. `New`/`NewWithName` panicked on bad env values. Errors are now returned.

`log`:

6. Data race in `contextLogger.Log`, which swapped `ctx` on a shared pointer. Loggers are immutable, and a race test covers it.
7. Float fields were logged as raw bit patterns in the GCP payload and in span attributes. Floats are now encoded as numbers.
8. Duration and Time span attributes were always empty. Logs no longer write span attributes.
9. `SetLevel` could only raise the level. `slog.LevelVar` moves both ways.
10. `With`/`Named` fields were dropped by the GCP backend. There is one backend now, and attributes always apply.
11. `Panic`/`Fatal` did not panic or exit in GCP-only mode. These methods are removed; the caller decides explicitly.
12. Env vars could not override code config, and the presets reset Level/Backend. Logging env is read only by `OptionsFromEnv`. For telemetry, `OTEL_*` env always wins over code.
13. Fallback warnings went to stdout through `fmt.Printf` and corrupted the JSON stream. Nothing is printed outside the logger.
14. Trace keys were not ones Cloud Logging recognises, so logs never linked to traces. The special `logging.googleapis.com/*` keys are now used.
15. Every ctx log added a span event, and every Error log set the span status to Error. Both are removed; use `otelx.RecordError` or `otelx.End`.
16. Fragile caller-skip bookkeeping. Source locations come from slog's PC; `gormlog` walks frames outside gorm.

`tracing`:

17. Calling `Register` twice leaked the previous provider, and the global was unsynchronised. `Setup` returns an owned `Telemetry` with an idempotent `Shutdown` that restores globals. No package-level state remains.
18. `Init` never returned an error. `Setup` does.
19. `UpdateSamplingRate` was a no-op. It is removed; sampling comes from config or env.
20. `HealthCheck` was cosmetic. It is removed.
21. `Shutdown`/`ForceFlush` checked for a nil ctx. Contexts are now required (D5).
22. Outdated semconv names: `http.method`, `http.url`, `db.name`, `db.statement`, `db.system="gorm"`, and `service.name` set as a span attribute. The helpers are removed; otelhttp/otelgrpc and the gorm plugin emit semconv v1.43-era names.
23. `StatsConfig.SkipMetrics` did nothing. It is removed.
24. `GormTracingConfig.EnableSlowQueryLogging`/`SlowQueryThreshold` were never read. `gormlog.Config.SlowThreshold` works.
25. `tracing` depended on `log` through a `replace` directive. No module depends on another kitchen module through `replace`.
26. Credentials JSON passed to the legacy Cloud Trace exporter used the unvalidated `option.WithCredentialsJSON`. Credential types are now validated before use (allow-list).

## Revision 2026-11-20 (owner review)

- **Default JSON keys changed.** The zero `Options` now writes `SchemaOTel`
  (`timestamp`, `level`, `message`, `source`, `trace_id`/`span_id`/`trace_flags`).
  On GCP use `Schema: log.SchemaGCP`, `LOG_SCHEMA=gcp`, or rely on detection
  (Cloud Run, Cloud Functions, App Engine, `GOOGLE_CLOUD_PROJECT`); on GKE set
  `LOG_SCHEMA=gcp`. `SchemaAWS` targets CloudWatch.
- `Options.GCPProjectID` and the `projects/<p>/traces/<id>` trace form are gone.
- `KeySeverity`, `KeyMessage`, `KeySourceLocation`, `KeyTrace`, `KeySpanID`,
  `KeyTraceSampled` became `KeyGCPSeverity`, `KeyGCPMessage`,
  `KeyGCPSourceLocation`, `KeyGCPTrace`, `KeyGCPSpanID`, `KeyGCPTraceSampled`;
  `KeySpanID` now names the OTel `span_id` key.
- `NewContextHandler(next, schema, extract)` replaces `NewContextHandler(next, gcpProjectID)`.
- Times are UTC.
- A user attribute named like a schema key (e.g. `message`, `severity`,
  `msg`, `level`) is written as `attr.<key>`.
- **zap is the default engine again, behind the slog API.** `log.New`
  returns `*slog.Logger`; `Options.Backend` / `LOG_BACKEND` choose `zap`
  (default) or `slog`. Code that wants zap's API uses `log.NewZap`, which
  writes the same schema but has no context (no trace keys).
- **Stack traces on ERROR records** under `exception.stacktrace` (OTel) or
  `stack_trace` (GCP, AWS); GCP Error Reporting picks them up. Turn off with
  `NoStackTrace` or move the threshold with `StackTraceLevel`.
- **Request id / user id**: automatic; `kitchen/context` stores them as log attributes of the request context
  replaces the old automatic gRPC metadata fields.
- `NewContextHandler(next, opts)` now takes `Options`.
- `sloglint` is part of the lint config: use `InfoContext(ctx, ...)` etc.
  when a context is in scope, constant messages, and never the global logger.

## Revision 2026-11-21 (group-2 review)

- `otelx/sdk`: Go runtime metrics are registered whenever metrics are enabled
  (`Config.NoRuntimeMetrics` to opt out); `OTEL_*_EXPORTER=console` is
  accepted; `Config.IgnoreRemoteSampled` for services facing untrusted callers.
- `otelx`: `PublicHTTPHandler` and `PublicGRPCServerHandler` for public
  ingress (the old `tracing` package always continued the caller's trace).
- `otelx/gcp`: `MetricExporter` and `LogExporter` send metrics and logs to the
  Cloud Telemetry API (Cloud Monitoring metrics appear under
  `prometheus.googleapis.com/…`).

