# otelx

OpenTelemetry for kitchen services. `main` sets it up once with
`otelx/sdk`; every other package (server, httpx, pubsub, redisx, pgx, ...)
then traces and meters through the global providers without extra wiring.
The core `otelx` package adds span helpers and HTTP/gRPC instrumentation.
It is the canonical telemetry package (D7 in
[docs/decisions.md](../docs/decisions.md)): do not call `otel.Set*` or build
`otelhttp`/`otelgrpc` handlers yourself.

| Package | Module | Use it for |
|---|---|---|
| `otelx` | core | spans in your code, propagation, HTTP/gRPC instrumentation |
| `otelx/sdk` | submodule | `Setup`: providers, OTLP exporters, sampler, globals |
| `otelx/gcp` | submodule | Google Cloud exporters (Telemetry API) and resource detection |
| `otelx/prometheus` | submodule | a Prometheus metric reader |
| `otelx/gcp/cloudtrace` | submodule | the legacy Cloud Trace exporter (deprecated) |

## Install

```sh
go get github.com/dungnh3/kitchen              # otelx
go get github.com/dungnh3/kitchen/otelx/sdk
go get github.com/dungnh3/kitchen/otelx/gcp          # on Google Cloud
go get github.com/dungnh3/kitchen/otelx/prometheus   # to keep /metrics
```

## Quick start

In `main`, set up logging, then telemetry, and shut telemetry down last
(from `sdk.ExampleSetup`):

```go
ctx := context.Background()
opts, _ := log.OptionsFromEnv()
logger, _ := log.Setup(opts) // the process logger; OpenTelemetry's errors go through it too
tel, err := sdk.Setup(ctx, sdk.Config{
	ServiceName:    "orders",
	ServiceVersion: "1.4.0",
	Environment:    "production",
	// Any OTLP backend; vendor auth is just a header.
	Traces: sdk.SignalConfig{
		Endpoint: "otel-collector.observability:4317",
		Insecure: true,
	},
})
if err != nil {
	logger.ErrorContext(ctx, "telemetry setup failed", log.Err(err))
	os.Exit(1)
}
defer func() { _ = tel.Shutdown(context.Background()) }()
```

The defaults are:

- traces go over OTLP/gRPC to `localhost:4317`; metrics and logs are off
  until you enable them;
- every new root trace is sampled. Health checks, reflection and Pub/Sub
  streaming pulls are never sampled;
- the W3C TraceContext and Baggage propagators are used;
- the providers, propagator and error handler are installed as globals.
  OpenTelemetry's own errors and diagnostics go to `Config.Logger`
  (`log.Default()`);
- Go runtime metrics are added whenever metrics are on.

Every standard `OTEL_*` variable wins over the code, so operators can
retarget telemetry without a rebuild.

Then, in your own code, add spans where they help (from `otelx.ExampleEnd`):

```go
func charge(ctx context.Context, amount int) (err error) {
	ctx, span := otelx.Start(ctx, "charge")
	defer otelx.End(span, &err) // records err and sets status Error when non-nil

	_ = ctx
	if amount <= 0 {
		return errors.New("invalid amount")
	}
	return nil
}
```

## Configuration

| `sdk.Config` field | Env var (wins) | Default | Change it when |
|---|---|---|---|
| `ServiceName`, `ServiceVersion`, `Environment` | `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | none | always set them |
| `Traces`, `Metrics`, `Logs` (`SignalConfig`) | `OTEL_<SIGNAL>_EXPORTER`, `OTEL_EXPORTER_OTLP[_<SIGNAL>]_{ENDPOINT,PROTOCOL,HEADERS,INSECURE,TIMEOUT}` | traces OTLP/gRPC, others `none` | a vendor endpoint, HTTP protocol, `console` locally |
| `SampleRatio` | `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | 1 | high traffic |
| `IgnoreRemoteSampled` | none | false | internet-facing services (see Advanced) |
| `ExcludeOperations` | none | health, reflection, StreamingPull | noisy endpoints |
| `SpanExporters`, `MetricReaders`, `LogProcessors` | none | none | Google Cloud, Prometheus, tests |
| `ResourceDetectors` | none | none | platform attributes (GCP) |
| `MetricInterval` | `OTEL_METRIC_EXPORT_INTERVAL` | 60s | faster dashboards |
| `NoRuntimeMetrics` | none | false | you export runtime metrics elsewhere |
| `NoGlobal` | none | false | tests that must not touch globals |
| `Disabled` | `OTEL_SDK_DISABLED` | false | turn everything off |
| `Logger` | none | `log.Default()` | pass the service logger |

## Integration with kitchen

`sdk.Setup` installs global providers, and every kitchen package picks them
up:

- `server` instruments its gRPC and HTTP ports;
- `httpx` traces outgoing requests;
- `pubsub` follows the messaging conventions;
- `redisx` and `pgx` trace commands and queries;
- `log` adds `trace_id`/`span_id` to every `*Context` log call.

Each of them also takes explicit providers (`WithTracerProvider` and
`WithMeterProvider`, or `pubsub.WithTelemetry`) for the rare case where
globals do not fit.

- **Logs over OTLP.** Add the OTel bridge as a `kitchen/log` sink, so one
  log call writes stdout JSON and OTLP (from `sdk.Example_otlpLogs`):

  ```go
  opts, _ := log.OptionsFromEnv()
  opts.Handlers = append(opts.Handlers, tel.SlogHandler("orders")) // stdout JSON + OTLP
  logger, _ := log.Setup(opts)
  logger.InfoContext(ctx, "started")
  ```

- **Errors.** `otelx.End` and `otelx.RecordError` record an error once and
  set the span status. Use them instead of `span.RecordError`, so status and
  event never disagree.
- **Propagation across queues.** `pubsub` already carries the trace context
  in message attributes. For your own carriers (from `otelx.ExampleInject`):

  ```go
  // Producer: write trace context into message attributes.
  attrs := propagation.MapCarrier{}
  otelx.Inject(context.Background(), attrs)

  // Consumer: continue the trace.
  ctx := otelx.Extract(context.Background(), attrs)
  _ = otelx.WithSpan(ctx, "handle message", func(_ context.Context) error { return nil })
  ```

- **Service wiring.** See [server/USAGE.md](../server/USAGE.md) for the full
  `main`: logging, telemetry, server and graceful shutdown.

## Advanced

### Internet-facing services

A client can send a `traceparent` header to pick your trace IDs and force
sampling. On public endpoints, ignore the caller's sampled flag (from
`sdk.Example_publicService`):

```go
ratio := 0.1
tel, err := sdk.Setup(context.Background(), sdk.Config{
	ServiceName:         "gateway",
	SampleRatio:         &ratio,
	IgnoreRemoteSampled: true,
	Metrics:             sdk.SignalConfig{Exporter: sdk.ExporterOTLP},
})
```

For handlers you serve yourself, `otelx.PublicHTTPHandler` and
`otelx.PublicGRPCServerHandler` start a new trace and only link the
caller's.

### HTTP and gRPC outside server

`server` already instruments both ports. Wrap other handlers yourself (from
`otelx.ExampleHTTPHandler`):

```go
// Internal traffic: continue the caller's trace.
internal := otelx.HTTPHandler(mux, "orders.http")
// Internet-facing: start a new trace and only link the caller's, so a
// client cannot choose your trace IDs or force sampling.
public := otelx.PublicHTTPHandler(mux, "orders.public")
```

For outgoing calls, prefer `httpx`, which already traces. Use these for gRPC
clients and for SDKs that take a RoundTripper (from
`otelx.ExampleGRPCClientHandler`):

```go
conn, err := grpc.NewClient("billing:9090",
	grpc.WithTransportCredentials(insecure.NewCredentials()), // TLS in production
	grpc.WithStatsHandler(otelx.GRPCClientHandler()))
if err != nil {
	return
}
defer func() { _ = conn.Close() }()

rt := otelx.HTTPTransport(http.DefaultTransport) // for an SDK's custom transport option
```

### Correlating logs by hand

`otelx.TraceID(ctx)` and `otelx.SpanID(ctx)` return the IDs, for example to
show a support reference to users. `kitchen/log` already writes them into
every `*Context` record.

## Backends and adapters

### otelx/gcp (Google Cloud)

Exporters for the Cloud Telemetry API (`telemetry.googleapis.com`), the path
Google recommends for Cloud Trace, Monitoring and Logging, authenticated with
Application Default Credentials (Workload Identity on GKE and Cloud Run).
From `gcp.ExampleTraceExporter`:

```go
cfg := gcp.Config{} // ADC / Workload Identity; project from the credentials or metadata server
texp, err := gcp.TraceExporter(ctx, cfg)
if err != nil {
	return
}
mexp, err := gcp.MetricExporter(ctx, cfg)
if err != nil {
	return
}
lexp, err := gcp.LogExporter(ctx, cfg)
if err != nil {
	return
}
exporters := []sdktrace.SpanExporter{texp}
readers := []sdkmetric.Reader{sdkmetric.NewPeriodicReader(mexp)}
processors := []sdklog.Processor{sdklog.NewBatchProcessor(lexp)}
detectors := []resource.Detector{gcp.ResourceDetector(cfg)} // adds gcp.project_id, required by the API
```

Pass them as `sdk.Config.SpanExporters`, `MetricReaders`, `LogProcessors`
and `ResourceDetectors`.

- Keep the resource detector: the API rejects data without
  `gcp.project_id`.
- Load `CredentialsJSON` from a secret, never from the repository.
- Set `DisableDetection` for local runs, to skip the metadata server.

### otelx/prometheus

Keep an existing `/metrics` scrape while moving to OpenTelemetry (from
`prometheus.ExampleNewReader`):

```go
reader, err := prometheus.NewReader(prometheus.Options{}) // prometheus.DefaultRegisterer
if err != nil {
	return
}
metrics := promhttp.Handler() // serves the default registry, now with OTel metrics
```

- Pass the reader as `sdk.Config.MetricReaders`.
- Serve the handler on the admin port with
  `server.WithAdmin(addr, server.WithAdminHandler("GET /metrics", metrics))`,
  never on the public port.
- Create one reader per registry, or every metric is exported twice.

### otelx/gcp/cloudtrace (deprecated)

The legacy Cloud Trace v2 exporter, kept for projects that cannot use the
Telemetry API yet. New services use `otelx/gcp`.

## Testing

- Inject in-memory exporters with `SpanExporters: []sdktrace.SpanExporter{tracetest.NewInMemoryExporter()}`
  and `MetricReaders: []sdkmetric.Reader{sdkmetric.NewManualReader()}`, then
  call `tel.ForceFlush(ctx)` before asserting.
- Set `NoGlobal: true` when tests run in parallel, and pass the providers to
  the code under test with its `WithTracerProvider` option.
- Set `Disabled: true` (or `OTEL_SDK_DISABLED=true`) to run without any
  telemetry.

## Observability

- **Resource**: `service.name`, `service.version`,
  `deployment.environment.name`, plus detector attributes.
- **Runtime metrics**: `go.memory.*`, `go.goroutine.count`,
  `go.processor.limit`, ... when metrics are on.
- **Span helpers**: spans from `otelx.Start` use the instrumentation scope
  `github.com/dungnh3/kitchen/otelx`.
- **Export failures** are logged once per error through `Config.Logger` as
  `opentelemetry error`.

## Production checklist

- Call `log.Setup`, then `sdk.Setup`, early in `main`. Defer
  `tel.Shutdown` with a fresh context so the last spans are flushed.
- Set `ServiceName`, `ServiceVersion` and `Environment`.
- Set `IgnoreRemoteSampled` and a `SampleRatio` on internet-facing services.
- Keep OTLP endpoints on TLS. `Insecure` is for in-cluster collectors only.
- Serve Prometheus metrics on the admin port only.
- Do not enable the Google client libraries' built-in tracing next to
  kitchen's; spans would be duplicated.

## Migrating from the old kitchen

The old `tracing` package becomes `otelx` and `otelx/sdk`; see
[docs/migration/observability.md](../docs/migration/observability.md).
