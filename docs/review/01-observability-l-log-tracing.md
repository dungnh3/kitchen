# Review: `l/`, `log/`, `tracing/` (read-only)

Paths are relative to `/Users/dungnh/works/sources/github.com/vulcan/kitchen`. I read every non-test source file. The `tracing` tests pass (`go test ./...` ok). `l` and `log` have **no tests at all**. I got dependency versions from the Go module proxy (`go list -m -u`, proxy.golang.org, run 2026-11-20).

## 0. What downstream actually uses (grep of ../*)
- **Migration is about halfway.** 745 files in 42 repos import `kitchen/l`; 1121 files in 45 repos import `kitchen/log`. 1086 of the `log` imports use the alias **`l`**, so field helpers (`l.String` / `l.Error` / `l.Object` / `l.Int`) are already source-compatible. 46 go.mods require both modules; 21 require only `l`.
- **Breaking points in files that still import `kitchen/l`:**
  - the `l.Logger` struct type (451 refs)
  - `l.New` (313) and `l.NewWithName` (24)
  - `.Ctx(ctx)` (42)
  - `.With(` (15), which returns `*zap.Logger`
  - `.S.Infof` (1)
  - `ErrorWithCtx` / `InfoWithCtx` exist in both packages.
- **`log` features nobody uses:** zero hits for `LOG_BACKEND`, `BackendGCP`, `BackendHybrid` or `GCP_LOG_NAME` anywhere downstream. `NewGormLogger` has 29 uses and `NewRestyLogger` 1.
- **`tracing` usage:**
  - Heavy: `StartSpan` 243, `RecordError` 171, `CreateEvent` 40, `Init` / `TracingHandle` ~33–41, `GlobalProvider` 34, `CreateEnhancedStatsHandler` 31.
  - Legacy: `RegisterTracer` 15.
  - Zero uses: `GormTracingPlugin`, `GormDatabaseWrapper`, `WithSpanAsync`, `AddBusinessLogic`, `MeasureLatency`, `UpdateSamplingRate`, `CloudTraceName`.
  - 27 repos carry their own `internal/tracing/tracing.go` or `internal/must/tracing.go` wrappers, all with different md5s. They define things like `IsTracingEnabled`, `EndSpan`, `HTTPTransport`, `GrpcStatsHandler` and `Attr*` constants, which shows what kitchen is missing.

## 1. `l` — legacy, should be dropped (superseded by `log`)
**API:** a `Logger` struct that embeds `*zap.Logger` plus `S` (sugar) and a private ctx (`l/log.go:29`). `New` / `NewWithName` read `LOG_LEVEL` / `LOG_ENCODER` and **panic** on a bad value (`log.go:66,95`). It also has field aliases (`fields.go`) and `Ctx`, `ErrorWithCtx`, `InfoWithCtx` and the gRPC `Log` adapter.

**Defects (these matter for deciding how fast to retire it):**
- **Every log through a `Ctx(ctx)` logger ends the caller's span** (`otel.go:52-53`, `defer span.End()`). This is a real production bug: spans get truncated at the first log line.
- `Ctx` drops `S`, so `logger.Ctx(ctx).S` is nil (`otel.go:20`).
- `Log()` ignores ctx, so gRPC interceptor logs carry no trace (`log.go:131`).
- `Error(err)` stores the error as a plain string, which loses the error type (`fields.go:41`).
- `fmt.Errorf(errAttr...)` uses a non-constant format string (`otel.go:70`).
- Dead code: `newCustomEncoder` returns nil (`custom_encoder.go:8`), `customEncodeLevel` is never used (`log.go:100`).
- `With` returns `*zap.Logger`, which loses the wrapper.
- Deps: go 1.22.5; otel v1.28.0 (latest v1.47.0); grpc v1.65.0 (latest v1.85.0); go-grpc-middleware v2.1.0 (latest v2.3.4); zap v1.27.0 (latest v1.28.0).

**What `log` must provide so the 42 `l` repos can migrate** (or a codemod must handle):
1. Constructors `New(opts ...)` and `NewWithName`, either as shims or rewritten to `MustNewLogger(nil).Named(name)`.
2. A concrete exported type, or a rewrite of `l.Logger` to `log.LoggerInterface` (451 sites).
3. `Ctx(ctx)` as an alias of `WithContext`.
4. `With(...)` returning the wrapper type.
5. **Behaviour parity decisions:**
   - Default level: `l` defaults to DEBUG (`l/log.go:16`), `log` to INFO (`log/config.go:15`), so debug logs silently disappear after migrating.
   - Output schema: `l` writes `time` (ISO8601), `severity` as WARN, `func`, `stacktrace`. `log`'s default encoder (`gcp`) writes `timestamp` (RFC3339), `severity` as WARNING, `function`, `stack_trace` (`log/encoders.go:26-54`). Any log-based metric or alert keyed on these fields will break.
   - `Error(err)`: `l` writes a string; `log` uses `zap.Error`, which also emits `errorVerbose` for formatter errors.

## 2. `log` — the intended successor; needs a redesign
**API:**
- `LoggerInterface` (`types.go:20`): 7 level methods, `WithContext`, `Info`/`Warn`/`ErrorWithCtx`, and the gRPC `Log` method.
- Constructors `NewLogger(cfg)`, `MustNewLogger`, `NewDefaultLogger` (`logger.go:17-105`).
- Backends `zap` / `gcp` / `hybrid` using the Cloud Logging client. Env vars `LOG_LEVEL`, `LOG_ENCODER`, `LOG_BACKEND`, `GCP_PROJECT_ID`, `GCP_LOG_NAME`, `LOG_DEVELOPMENT`.
- On every ctx log it adds trace fields, a gRPC metadata allow-list (`types.go:142`) and a **span event**.
- Also provides `GormLogger` and `RestyLogger`.
- No package-level global, but downstream (and kitchen itself) build package-level loggers with `NewDefaultLogger()` in `var` blocks, e.g. `grpc/gatewayopt/gateway_opt.go:21`, `pubsub/kafka_sarama/consumer.go:11`, `server/server.go:20`.

**Bugs:**
- **Data race:** `contextLogger.Log` temporarily swaps `cl.ctx` on a shared pointer (`context.go:288-291`).
- **Float values come out as raw bits:** float fields are logged as their bit pattern: `fields.go:229-234` returns `field.Integer` for Float32/Float64 (GCP payload), and `context.go:169-170` does `float64(field.Integer)` (span attributes).
- Duration and Time span attributes read `field.String`, which is always empty (`context.go:171-174`).
- **`SetLevel` can only raise the level:** it uses `zap.IncreaseLevel` and never lowers (`logger.go:403-409`).
- `With` and `Named` only affect zap. **The GCP backend drops attached fields** (`logger.go:422-440`).
- In GCP-only mode, `Panic` and `Fatal` neither panic nor exit (`logToGCP`, `logger.go:305-327`).
- **Env vars cannot override code config:**
  - `MergeConfig(env, cfg)` lets the cfg passed in code win over env vars (`logger.go:42-43`).
  - `Development` is always taken from the override, so `LOG_DEVELOPMENT` is ignored whenever a cfg is passed (`config.go:114`).
  - `NewGCPConfigFromEnv` merges a full default env config over the preset, which resets Backend to `zap` and Level to INFO (`config.go:391-392` together with `39-40`). The presets are effectively dead.
- Fallback warnings go through `fmt.Printf` to stdout and corrupt the JSON log stream (`logger.go:72,81,87`).
- **No trace link in Cloud Logging:**
  - The key is `trace_id` / `span_id` (`types.go:111-112`). Cloud Logging only links logs to traces via `logging.googleapis.com/trace` and `/spanId`, and only treats `message` (not `msg`, which `types.go:116` uses) as the message field. Source: https://docs.cloud.google.com/logging/docs/structured-logging
  - The GCP `Entry` never sets `Trace` / `SpanID` either.
  - Whether Cloud Logging accepts `timestamp` as an RFC3339 string is unverified.
- **Design smell:**
  - Every ctx log adds a span event, and every Error-level log sets the span status to Error (`context.go:115-119`). At 30K CCU this doubles log volume into traces, and "logged an error" is not the same as "span failed".
  - Caller-skip bookkeeping (`AddCallerSkip(3)` plus per-wrapper skips, `logger.go:59`, `gorm_logger.go:68-73`) is fragile.
- **Dead code:** `isGCPAccessible`, `adjustBackendForGCPFailure` (`logger.go:109-138`), `defaultEncoderConfig`, `customEncodeLevel` (`encoders.go:186-224`), `errKey`.
- **Duplication:** the allow-list and `ExtractFieldsFromCtx` are copied from `l/otel.go:111-135`.

**Deps:**

| Module | Used | Latest |
|---|---|---|
| cloud.google.com/go/logging | v1.13.0 | v1.21.1 |
| otel / otel/trace | v1.39.0 | v1.47.0 |
| grpc | v1.75.1 | v1.85.0 |
| go-grpc-middleware/v2 | v2.3.2 | v2.3.4 |
| zap | v1.27.0 | v1.28.0 |
| gorm | v1.31.0 | v1.31.2 |

The GORM logger forces `gorm` into every consumer of the logging module.

## 3. `tracing` — keep, slim down and update
**API:**
- `Register(log, svc, opts...)` with a `Backend` interface; implementations Coralogix, GCP (OTLP to telemetry.googleapis.com, or legacy Cloud Trace), OTLP and Tempo.
- `Init(ctx, log, svc, *InitConfig)` returns a `TracingHandle`. It never fails: errors are swallowed and the returned `error` is always nil (`init.go:296-304`).
- Legacy `RegisterTracer` / `Config` (Coralogix only, `tracing.go:99`).
- Sampler chain: operation filter, then ParentBased(ratio) (`filter.go`).
- otelgrpc stats-handler presets, propagation helpers, span helpers, a GORM plugin.
- **Globals:** `otel.SetTracerProvider`, the propagator and the error handler (`provider.go:296-345`), plus an unsynchronized `globalProvider` (`provider.go:365`) and `captureCodeAttributes`. Calling Register twice leaks the previous provider, which is never shut down.

**Issues:**
- `UpdateSamplingRate` is a no-op that misleads callers: it only mutates the config (`tracing.go:160-166`).
- `HealthCheck` is cosmetic (`tracing.go:133`).
- `Shutdown` and `ForceFlush` check for a nil ctx (`tracing.go:176,210`).
- **Outdated semantic-convention names** in `AddHTTPInfo` / `AddDatabaseInfo` / the GORM plugin (`helpers.go:105-121`, `gorm_plugin.go:182-245`):
  - `http.method` / `http.url` / `http.status_code` / `db.name` / `db.statement` (current: `http.request.method`, `url.full`, `http.response.status_code`, `db.namespace`, `db.query.text`)
  - `db.system="gorm"` (`gorm_plugin.go:182`)
  - `service.name` set as a span attribute (`helpers.go:55`, `gorm_plugin.go:189`)
- Semconv package v1.39.0 is imported (`resource.go:10`, `helpers.go:13`); the newest in otel v1.47.0 is v1.43.0.
- **Six overlapping stats-handler constructors** (`stat.go:121-262`). `SkipMetrics` does nothing (`stat.go:35-40`).
- `GormTracingConfig.EnableSlowQueryLogging` / `SlowQueryThreshold` are never read.
- **Superseded duplicates:**
  - `RegisterTracer` + `Config` (superseded by `Register` / `Init`)
  - `GormDatabaseWrapper` + `*WithTracing` (0 uses)
  - the handler presets
  - the hand-written GORM plugin (`gorm.io/plugin/opentelemetry` v0.1.16 exists)
- Depends on `kitchen/log` through a `replace ../log` (`go.mod:5`), which couples tracing's release cycle to the logger.
- Tracing only; there are no metrics or logs signals.

**Deps** (otel v1.43.0 everywhere unless noted):

| Module | Used | Latest |
|---|---|---|
| otel core (otel, sdk, trace, otlptrace grpc/http) | v1.43.0 | v1.47.0 |
| otelgrpc | v0.63.0 | v0.72.0 |
| contrib/detectors/gcp | v1.43.0 | v1.47.0 |
| GCP cloudtrace exporter | v1.32.0 | v1.39.0 |
| google.golang.org/api | v0.249.0 | v0.316.0 |
| grpc | v1.80.0 | v1.85.0 |
| x/oauth2 | v0.35.0 | v0.38.0 |
| proto/otlp | v1.10.0 | v1.11.1 |
| compute/metadata | v0.9.0 | v0.10.0 |
| coralogix-opentelemetry-go | v1.4.7 | latest per proxy (last release date unverified) |

## 4. Modern direction and recommendations
**Current ecosystem state:**
- `log/slog` (stdlib) is the standard logging API.
- OTel's log API `go.opentelemetry.io/otel/log` is at v1.47.0, the SDK `otel/sdk/log` at v1.47.0, `otlploggrpc` still at v0.23.0.
- Bridges: `contrib/bridges/otelslog` v0.21.0 and `otelzap` v0.21.0.
- Declarative config: `contrib/otelconf` v0.27.0. Env-driven exporters: `contrib/exporters/autoexport` v0.72.0.
- `otelhttp` v0.72.0.
- go-grpc-middleware v2 `logging` takes any `Log(ctx, level, msg, ...any)` adapter, so a ~10-line slog adapter is enough.

**`l` → DROP.** Freeze it; release a final `l` version whose internals forward to `log` (keeping `New`, `Logger`, `Ctx`). Remove the `span.End()` bug immediately if `l` keeps shipping. Provide a codemod for the 42 repos.

**`log` → REDESIGN**, slog-first:
```go
func New(opts ...Option) *slog.Logger                     // JSON handler on stdout, GCP field mapping
func NewHandler(w io.Writer, opts ...Option) slog.Handler // ReplaceAttr: level->severity, msg->message
// ContextHandler wrapper: adds logging.googleapis.com/trace + /spanId + allow-listed gRPC metadata
func WithOTel(h slog.Handler, lp log.LoggerProvider) slog.Handler // fan-out to otelslog
func GRPCLogger(l *slog.Logger) logging.Logger
// subpackages so the core module has no gorm/resty deps:
//   log/gormlog, log/restylog, log/zapcompat
//   (zapcompat: Field helpers String/Int/Error/Object, used by ~6.4k l.String calls)
```
Drop the direct Cloud Logging client backend (stdout JSON is ingested by the GKE / Cloud Run agents). Drop span events per log, presets, and the hybrid/fallback logic. Use `slog.LevelVar` for runtime levels.

**`tracing` → UPDATE deps now, REDESIGN in the new repo** into an `otel` (telemetry) module:
- `Setup(ctx, Config) (shutdown func(ctx) error, err error)` building Trace, Meter and Logger providers, honouring `OTEL_*` env and/or `otelconf`.
- Backends reduced to OTLP plus GCP-auth and Coralogix-header presets.
- Keep the operation-filter sampler, `MapCarrier` and the propagation helpers.
- `StartSpan` / `RecordError`, plus exported `Attr*` keys and `HTTPTransport()` (otelhttp) to absorb the 27 per-repo wrappers.
- One `ServerHandler(opts)` instead of six.
- Update semconv to v1.43.0.
- Drop `RegisterTracer`, `GormDatabaseWrapper`, `UpdateSamplingRate`, `HealthCheck`; use `gorm.io/plugin/opentelemetry` or a slim plugin.
- Remove the dependency on `kitchen/log` (accept `*slog.Logger`).

## 5. Open questions for the owner
1. Which tracing backends are live in prod per environment (GCP OTLP, Cloud Trace legacy, Coralogix, Tempo)? Is Coralogix being retired (`init.go:207` warns that it is no longer the default)?
2. Do any dashboards, alerts or log-based metrics depend on the current JSON keys (`time` vs `timestamp`, `msg`, `severity` WARN vs WARNING, `trace_id`)? This decides whether the new schema can switch to `message` + `logging.googleapis.com/*`.
3. Is the Cloud Logging API backend (`BackendGCP` / `BackendHybrid`) used anywhere outside these sibling repos (Cloud Functions, jobs outside GKE)? Grep shows zero use here.
4. Should logs also be exported via OTLP (otelslog to the collector), or stay stdout plus agent only?
5. Is a span event per log line and span status = Error on any Error log relied on in trace views? (I recommend removing both.)
6. Is a stable zap-style field API (`l.String`, `l.Object`, ~13k call sites) required, or is a codemod to slog `slog.String` / `slog.Any` acceptable?
7. Is the date-in-patch tag scheme (`log/v1.0.20251210`) going to continue in the new repo, and should the module path change to `github.com/dungnh3/kitchen/...`?
8. Is a metrics signal in scope for the new telemetry module (exemplars need traces and metrics from the same SDK)?
