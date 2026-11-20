# Design: observability (`log`, `otelx`)

Status: approved by the owner on 2026-11-20. Questions were asked in six popup rounds; the owner then said "Đồng ý với tất cả Recommended". Decisions the owner was never asked about are listed under [Decisions taken by default](#decisions-taken-by-default-to-review).

This area replaces the old packages `l`, `log` and `tracing`. Findings about the old code are in `docs/review/01-observability-l-log-tracing.md`.

Two cross-area owner directives also apply here: the code contains no product or company names, and this is a personal library, so downstream usage is not a design input.

Per the owner, every bug listed in the review is fixed in the new code and none is carried over. Each fix is listed as a behaviour change in `docs/migration/observability.md`.

## Layout

| Import path | Module | Contents | Dependencies |
|---|---|---|---|
| `github.com/dungnh3/kitchen/log` | core | `*slog.Logger` factory, GCP JSON schema, context handler, ctx attrs, levels | stdlib, `otel/trace` |
| `github.com/dungnh3/kitchen/otelx` | core | API-only helpers: spans, errors, propagation, gRPC/HTTP instrumentation | `otel` API, `otelgrpc`, `otelhttp`, `grpc` |
| `github.com/dungnh3/kitchen/otelx/sdk` | submodule | `Setup`: SDK providers for traces, metrics and logs, OTLP exporters, sampler, `otelslog` bridge | `otel/sdk*`, `otlp*`, `otelslog` |
| `github.com/dungnh3/kitchen/otelx/gcp` | submodule | Google Cloud: Telemetry API OTLP exporter with ADC, credential/project resolution, GCP resource detector (`gcp.project_id` + platform) | `oauth2/google`, `otlptracegrpc`, `detectors/gcp` |
| `github.com/dungnh3/kitchen/otelx/gcp/cloudtrace` | submodule | **Deprecated** legacy Cloud Trace v2 exporter (upstream archived after 2027-01-01) | `opentelemetry-operations-go/exporter/trace` |
| `github.com/dungnh3/kitchen/otelx/prometheus` | submodule | OTel metric reader exposed through a Prometheus registry | `otel/exporters/prometheus`, `client_golang` |
| `github.com/dungnh3/kitchen/log/gormlog` | submodule | GORM `logger.Interface` on `*slog.Logger` | `gorm.io/gorm` |

The old `l` package is dropped. `log` replaces it.

The submodules do not import the core module. They exchange only plain OTel/SDK types (`sdktrace.SpanExporter`, `sdkmetric.Reader`, `resource.Detector`, `*slog.Logger`). That means:
- no cross-module version pinning;
- no `replace` directives (D1);
- each module can be released on its own.

The package is named `otelx`, not `otel` as D6 proposed, so it does not clash with `go.opentelemetry.io/otel`.

## Dependency versions (verified 2026-11-20 against proxy.golang.org)

| Module | Version |
|---|---|
| otel, otel/trace, otel/metric, otel/sdk, sdk/metric, otlptrace*, otlpmetric* | v1.47.0 |
| otel/log, otel/sdk/log | v1.47.0 (Logs API/SDK v1) |
| otlploggrpc, otlploghttp | v0.23.0 (experimental) |
| semconv package | v1.43.0 (newest inside otel v1.47.0) |
| contrib otelgrpc, otelhttp | v0.72.0 |
| contrib bridges/otelslog | v0.21.0 |
| contrib detectors/gcp | v1.47.0 |
| otel/exporters/prometheus | v0.69.0 |
| GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace | v1.39.0 |
| golang.org/x/oauth2 | v0.38.0 |
| gorm.io/gorm | v1.31.x |
| go.uber.org/goleak | v1.3.0 |

## Decisions (asked and approved by the owner)

| # | Topic | Decision |
|---|---|---|
| O1 | Log JSON schema | GCP-native: `time` (RFC3339Nano), `severity` (`DEBUG`/`INFO`/`NOTICE`/`WARNING`/`ERROR`/`CRITICAL`/`ALERT`/`EMERGENCY`), `message`, `logging.googleapis.com/trace`, `logging.googleapis.com/spanId`, `logging.googleapis.com/trace_sampled`, `logging.googleapis.com/sourceLocation`. No duplicate `trace_id`/`span_id`. |
| O2 | Tracing backends | Generic OTLP (Tempo, collector, or any vendor that accepts OTLP plus headers), the GCP Telemetry API (OTLP), and the legacy Cloud Trace exporter. Third-party exporters can be plugged in. |
| O3 | Cloud Logging API backend | Dropped. Logs are JSON on stdout, and the platform agent ingests them. |
| O4 | OTLP log export | Stdout by default. OTLP logs are opt-in through `otelx/sdk`, using the `otelslog` bridge combined with `slog.NewMultiHandler`. |
| O5 | Extensibility / vendors | Generic OTLP with endpoint and headers, plus injectable `SpanExporters`/`MetricReaders`/`LogProcessors` and resource attrs/detectors. There are no vendor presets: a vendor such as Coralogix is just OTLP with a header, documented as a recipe. A vendor that needs its own SDK gets an `otelx/<vendor>` submodule. |
| O6 | Span event per log, and span status Error on an Error log | Both removed. Mark a span failed explicitly with `otelx.RecordError` or `otelx.End(span, &err)`. |
| O7 | zap-style field API | No compatibility layer. Call sites are codemodded to `slog.*`; the only extra helper is `log.Err(err)`. |
| O8 | Metrics | In scope. `otelx/sdk` builds a `MeterProvider`; readers are pluggable (OTLP in `sdk`, Prometheus in `otelx/prometheus`). |
| O9 | Module split | Covered in [Layout](#layout): API helpers in core, SDK and vendors in submodules. |
| O10 | Config and env | Setup takes a `Config` struct whose zero value has documented defaults. Standard `OTEL_*` env vars override code. |
| O11 | Globals | `Setup` sets the global TracerProvider, MeterProvider, propagator and error handler by default. `Config.NoGlobal` disables this. `Shutdown` restores the previous globals. |
| O12 | Log level/format | Default is INFO + JSON. `log.New` never reads env; `log.OptionsFromEnv()` reads `LOG_LEVEL`, `LOG_FORMAT` and `GOOGLE_CLOUD_PROJECT`. The level is a `*slog.LevelVar`, so it can change at runtime. |
| O13 | Package name | `otelx`. |
| O14 | gRPC/HTTP instrumentation | Lives in core `otelx`: `GRPCServerHandler`, `GRPCClientHandler`, `HTTPHandler`, `HTTPTransport`. By default they skip `grpc.health.v1` and gRPC reflection. |
| O15 | GORM and Resty | GORM logging moves to submodule `log/gormlog`. GORM tracing uses `gorm.io/plugin/opentelemetry`. The Resty logger is dropped. |
| O16 | Request metadata in logs | Generic `log.WithAttrs(ctx, attrs...)`: the handler adds the ctx attrs to every `*Context` call. Transport interceptors decide which headers to copy in. `log` does not depend on grpc. |
| O17 | Sampler | Operation-filter sampler, then `ParentBased(TraceIDRatio(r))`. `r` defaults to 1.0, comes from `Config.SampleRatio`, and is overridden by `OTEL_TRACES_SAMPLER(_ARG)`. The default exclusions are Pub/Sub `StreamingPull`, `grpc.health.v1.Health/` and `grpc.reflection.`. |
| O18 | Setup failure | `Setup` returns an error for any bad config, credentials or exporter construction. `Config.Disabled` or `OTEL_SDK_DISABLED=true` returns a no-op `Telemetry`. Runtime export errors go to the OTel error handler, which logs to the configured `*slog.Logger`. |
| O19 | Error attribute | `log.Err(err)` writes `"error": err.Error()`. A nil error produces an empty attr, which handlers drop. |
| O20 | Source location | On by default (`logging.googleapis.com/sourceLocation`). `Options.NoSource` turns it off. |
| O21 | Trace value format | `projects/<id>/traces/<trace>` when `Options.GCPProjectID` is set, otherwise the bare trace id. |
| O22 | go-grpc-middleware logging adapter | Owned by the transport area. `log` exposes nothing grpc-specific. |
| O23 | Default OTLP protocol | `grpc`. Overridden by `OTEL_EXPORTER_OTLP_[SIGNAL_]PROTOCOL`. |
| O24 | Prometheus registry | `otelx/prometheus.Options.Registerer`; nil means `prometheus.DefaultRegisterer`. |

## Target API

### `log` (core)

```go
package log

const (
    LevelDebug     = slog.LevelDebug // DEBUG
    LevelInfo      = slog.LevelInfo  // INFO
    LevelNotice    = slog.Level(2)   // NOTICE
    LevelWarn      = slog.LevelWarn  // WARNING
    LevelError     = slog.LevelError // ERROR
    LevelCritical  = slog.Level(12)  // CRITICAL
    LevelAlert     = slog.Level(16)  // ALERT
    LevelEmergency = slog.Level(20)  // EMERGENCY
)

type Format string // FormatJSON (default) | FormatText

type Options struct {
    Level        slog.Leveler // nil → INFO; pass *slog.LevelVar for runtime changes
    Format       Format       // "" → JSON
    NoSource     bool         // disable sourceLocation
    GCPProjectID string       // enables projects/<id>/traces/<trace>
    ReplaceAttr  func(groups []string, a slog.Attr) slog.Attr // applied to user attrs before GCP mapping
}

func New(w io.Writer, opts Options) *slog.Logger
func NewHandler(w io.Writer, opts Options) slog.Handler          // ContextHandler(JSON/Text handler)
func NewContextHandler(next slog.Handler, gcpProjectID string) slog.Handler // adds trace + ctx attrs at top level
func Severity(l slog.Level) string
// Keys: KeySeverity, KeyMessage, KeySourceLocation, KeyTrace, KeySpanID, KeyTraceSampled, KeyError
func OptionsFromEnv() (Options, error)                           // LOG_LEVEL, LOG_FORMAT, GOOGLE_CLOUD_PROJECT
func ParseLevel(s string) (slog.Level, error)
func Err(err error) slog.Attr
func WithAttrs(ctx context.Context, attrs ...slog.Attr) context.Context
func AttrsFromContext(ctx context.Context) []slog.Attr
```

Example output:

```json
{"time":"2026-11-20T10:00:00.123456789Z","severity":"WARNING","logging.googleapis.com/sourceLocation":{"file":"/src/svc/x.go","line":"42","function":"svc.Do"},"message":"quota low","logging.googleapis.com/trace":"projects/p/traces/4bf92f3577b34da6a3ce929d0e0e4736","logging.googleapis.com/spanId":"00f067aa0ba902b7","logging.googleapis.com/trace_sampled":true,"x-request-id":"r1","user_id":"u1"}
```

Trace attrs and ctx attrs always stay at top level, even when the logger has `WithGroup`. The context handler re-nests grouped attrs itself.

### `otelx` (core, API only)

```go
func Tracer() trace.Tracer                       // global provider, scope "github.com/dungnh3/kitchen/otelx"
func Start(ctx, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span)
func RecordError(span trace.Span, err error, opts ...trace.EventOption) // records + status Error; nil no-op
func End(span trace.Span, errp *error)            // defer otelx.End(span, &err)
func WithSpan(ctx, name string, fn func(context.Context) error, opts ...trace.SpanStartOption) error // recovers panics: records, re-panics
func Inject(ctx, carrier propagation.TextMapCarrier)
func Extract(ctx, carrier propagation.TextMapCarrier) context.Context
func TraceID(ctx) string; func SpanID(ctx) string

func GRPCServerHandler(opts ...otelgrpc.Option) stats.Handler   // filters health + reflection by default
func GRPCClientHandler(opts ...otelgrpc.Option) stats.Handler
func HTTPHandler(h http.Handler, operation string, opts ...otelhttp.Option) http.Handler
func HTTPTransport(base http.RoundTripper, opts ...otelhttp.Option) http.RoundTripper
```

### `otelx/sdk` (submodule)

```go
type Exporter string // "otlp" | "none"
type Protocol string // "grpc" (default) | "http/protobuf"

type SignalConfig struct {
    Exporter Exporter          // default: traces "otlp" (or "none" when SpanExporters set), metrics "none", logs "none"
    Protocol Protocol
    Endpoint string            // "host:port" or full URL
    Headers  map[string]string
    Insecure bool
    Timeout  time.Duration
}

type Config struct {
    Disabled          bool
    NoGlobal          bool
    Logger            *slog.Logger // OTel error handler target; nil → slog.Default()
    ServiceName, ServiceVersion, Environment string
    ResourceAttrs     []attribute.KeyValue
    ResourceDetectors []resource.Detector
    Traces, Metrics, Logs SignalConfig
    SampleRatio       *float64  // nil → 1.0
    ExcludeOperations []string  // added to the default exclusions
    SpanExporters     []sdktrace.SpanExporter // batched
    MetricReaders     []sdkmetric.Reader
    LogProcessors     []sdklog.Processor
    MetricInterval    time.Duration // OTLP periodic reader interval; 0 → SDK default/env
}

type Telemetry struct{ /* TracerProvider, MeterProvider, LoggerProvider, Propagator */ }
func Setup(ctx context.Context, cfg Config) (*Telemetry, error)
func (*Telemetry) TracerProvider() trace.TracerProvider
func (*Telemetry) MeterProvider() metric.MeterProvider
func (*Telemetry) LoggerProvider() otellog.LoggerProvider
func (*Telemetry) Propagator() propagation.TextMapPropagator
func (*Telemetry) SlogHandler(scope string) slog.Handler // otelslog bridge on this LoggerProvider
func (*Telemetry) ForceFlush(ctx context.Context) error
func (*Telemetry) Shutdown(ctx context.Context) error    // idempotent; restores previous globals
func NewSampler(ratio float64, exclude []string) sdktrace.Sampler
var DefaultExcludedOperations []string
```

How env overrides code: when an `OTEL_*` variable that maps to a field is set, Setup leaves that field's option unset, so the SDK or exporter reads the env var itself with full spec semantics. This applies to the generic and signal-specific forms of `ENDPOINT`, `HEADERS`, `INSECURE`, `TIMEOUT` and `PROTOCOL`, and to `OTEL_{TRACES,METRICS,LOGS}_EXPORTER`, `OTEL_TRACES_SAMPLER(_ARG)`, `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SDK_DISABLED`.

### `otelx/gcp` (submodule)

```go
type Config struct {
    ProjectID        string // "" → credentials / metadata server
    CredentialsJSON  []byte // service_account | authorized_user | external_account | impersonated_service_account only
    CredentialsFile  string
    Endpoint         string // default "telemetry.googleapis.com:443"
    Timeout          time.Duration // 0 → 10s
    DisableDetection bool   // skip platform detector + metadata server
}
func TraceExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) // OTLP/gRPC + TLS + OAuth
func ResourceDetector(cfg Config) resource.Detector // gcp.project_id (required by the Telemetry API) + platform attrs
func Credentials(ctx context.Context, cfg Config) (*google.Credentials, error)
func ProjectID(ctx context.Context, cfg Config, creds *google.Credentials) (string, error)
```

### `otelx/gcp/cloudtrace` (submodule, deprecated)

```go
type Config struct{ ProjectID string; Credentials *google.Credentials; Timeout time.Duration }
func New(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error)
```

### `otelx/prometheus` (submodule)

```go
type Options struct {
    Registerer        prometheus.Registerer // nil → prometheus.DefaultRegisterer
    WithoutTargetInfo bool
    WithoutScopeInfo  bool
}
func NewReader(opts Options) (sdkmetric.Reader, error)
```

### `log/gormlog` (submodule)

```go
type Config struct {
    Logger               *slog.Logger  // nil → slog.Default()
    Level                logger.LogLevel // 0 → logger.Warn
    SlowThreshold        time.Duration // 0 → 200ms; <0 disables slow-query logs
    IgnoreRecordNotFound bool
    LogParams            bool          // false → SQL logged with placeholders (no values)
}
func New(cfg Config) *Logger // implements gorm logger.Interface and gorm.ParamsFilter
// Keys: db.query.text, db.response.returned_rows, elapsed, slow_threshold
```

`gormlog` takes the source location from the first stack frame outside `gorm.io/*` and outside `gormlog`, instead of doing caller-skip bookkeeping.

## Decisions taken by default (to review)

The owner approved all "(Recommended)" options and asked that new questions be settled with the option that is best for system design and security, recorded here.

| # | Decision | Alternatives | Why |
|---|---|---|---|
| DD1 | GCP severities above ERROR are custom slog levels: `LevelNotice`=2, `LevelCritical`=12, `LevelAlert`=16, `LevelEmergency`=20. Between them, levels map down to the nearest. | (a) Only the 4 slog levels; (b) a separate API per severity. | Covers the whole LogSeverity enum with plain `logger.Log(ctx, log.LevelCritical, ...)`. Cost: four extra constants. |
| DD2 | `OptionsFromEnv` returns `(Options, error)`; an invalid `LOG_LEVEL` or `LOG_FORMAT` is an error. This differs from the single-value preview in the popup. | Silently fall back to the default; panic (as the old `l` did). | D5 forbids panics, and a silent fallback hides misconfiguration. |
| DD3 | Submodules never import core packages; they exchange OTel/slog types only. | Import core and pin a core version. | No core tag exists yet, and D1 forbids `replace`. This also keeps modules releasable on their own. |
| DD4 | Default exporters: traces `otlp` (but `none` when `SpanExporters` are injected), metrics `none`, logs `none`. | All `otlp` (the spec default). | Avoids accidental export to `localhost:4317` and error spam when no collector exists. Metrics and logs are opt-in, matching O4 and O8. |
| DD5 | The propagator is W3C TraceContext + Baggage. | autoprop/`OTEL_PROPAGATORS`; adding the GCP `X-Cloud-Trace-Context` header. | Standard, and needs no extra dependency. Google front ends also accept `traceparent`. |
| DD6 | `gormlog` logs SQL with placeholders by default (`LogParams=false`); parameters are never logged unless opted in. Default level is Warn, slow threshold is 200ms. | Log interpolated SQL, as the old code did. | Security: query parameters often hold PII or secrets. |
| DD7 | `FormatText` (development) keeps slog's text keys but uses GCP severity names. Trace and ctx attrs are still added. | Same GCP keys in text mode. | Readability in a terminal. Production uses JSON. |
| DD8 | `ReplaceAttr` from `Options` runs on user attrs only; the GCP key mapping always wins for built-in keys. | Let users override the schema. | The schema is a contract (O1). |
| DD9 | `otelx.WithSpan` records a recovered panic as an error and re-panics. | Swallow the panic. | Never hide a crash. |
| DD10 | `otelx/gcp` accepts only the four credential JSON types listed above (`CredentialsFromJSONWithType`). | `google.CredentialsFromJSON`, which is deprecated. | Security: unvalidated external credential configs can make the process fetch arbitrary URLs or files. |
| DD11 | `Setup` sets the global error handler only when `NoGlobal` is false. The handler logs at ERROR with key `error` through `Config.Logger`. | Always set it; never set it. | Consistent with O11. |
| DD12 | Log-to-span behaviour is never implicit (O6). `otelx.RecordError` adds no custom `error.message`/`error.description` attrs; the exception event already carries `exception.message` and `exception.type`, per current semconv. | Keep the old custom attrs. | Current semconv; less duplication. |
| DD13 | The old `MapCarrier` is not reintroduced; use `propagation.MapCarrier` and `propagation.HeaderCarrier` from the OTel API. | Re-export them. | No reason to duplicate the stdlib/OTel API (D2). |
| DD14 | The old helpers `AddUserInfo`, `AddHTTPInfo`, `AddDatabaseInfo`, `CreateEvent`, `MeasureLatency`, `AddBusinessLogic` and `WithSpanAsync` are dropped. Use `span.SetAttributes(semconv.*)` and `span.AddEvent` directly. | Port them with updated semconv. | They wrapped one-line OTel calls and used outdated attribute names. |
| DD15 | The legacy Cloud Trace exporter (kept because of O2) lives in its own deprecated submodule `otelx/gcp/cloudtrace`. `otelx/gcp` exports `Credentials`/`ProjectID` so the submodule needs no import of it. | Keep it inside `otelx/gcp`; drop it. | We found during implementation that upstream deprecated the module and will archive it after 2027-01-01. Isolating it keeps the deprecated dependency out of `otelx/gcp`, and the module can be deleted later without touching anything else. |
| DD16 | The `gormlog` attribute keys follow current DB semconv: `db.query.text` and `db.response.returned_rows`. `elapsed` is a duration. | The old `sql`/`rows`/`elapsed` keys. | Current semconv. Queries on the old keys need updating; this is listed in the migration guide. |
| DD17 | `Shutdown` restores the previous globals. The OTel global delegate is bound once, so code that cached a tracer from the original global keeps a tracer that is no-op after shutdown. | Never restore. | Clean test isolation. The process exits after shutdown anyway. |
| DD18 | When `ServiceName` and `OTEL_SERVICE_NAME` are both empty, the service name is `unknown_service:<binary>`, as the spec says. | Error. | Matches the spec. |
