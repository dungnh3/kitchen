// Package sdk installs the OpenTelemetry SDK for an application: trace, metric
// and log providers, OTLP exporters, the sampler, the propagator and the error
// handler.
//
// Call [Setup] once in main and Shutdown before exit. Libraries never import
// this package; they use the OpenTelemetry API (or package otelx) and pick up
// the providers installed here.
//
// Configuration comes from [Config]; the standard OTEL_* environment variables
// override it, so operators can retarget or resample without a code change.
// Vendor backends are plain OTLP with headers, or injected exporters/readers
// (see the otelx/gcp and otelx/prometheus modules).
package sdk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	otelruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	lognoop "go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/dungnh3/kitchen/log"
)

// Telemetry holds the providers built by [Setup]. All methods are safe for
// concurrent use and safe on a no-op Telemetry.
type Telemetry struct {
	tp         trace.TracerProvider
	mp         metric.MeterProvider
	lp         otellog.LoggerProvider
	propagator propagation.TextMapPropagator

	flushers  []func(context.Context) error
	shutdowns []func(context.Context) error
	restore   func()

	once        sync.Once
	shutdownErr error
}

// TracerProvider returns the tracer provider.
func (t *Telemetry) TracerProvider() trace.TracerProvider { return t.tp }

// MeterProvider returns the meter provider (no-op when metrics are disabled).
func (t *Telemetry) MeterProvider() metric.MeterProvider { return t.mp }

// LoggerProvider returns the OTel logger provider (no-op when logs are
// disabled).
func (t *Telemetry) LoggerProvider() otellog.LoggerProvider { return t.lp }

// Propagator returns the text map propagator (W3C TraceContext + Baggage).
func (t *Telemetry) Propagator() propagation.TextMapPropagator { return t.propagator }

// SlogHandler returns a log.Handler (otelslog bridge) that emits records to
// the OTel logger provider under the given instrumentation scope. To export
// logs over OTLP as well as stdout, add it as a kitchen/log sink in main:
//
//	opts.Handlers = append(opts.Handlers, tel.SlogHandler("app"))
//	logger, err := log.Setup(opts)
func (t *Telemetry) SlogHandler(scope string) log.Handler {
	return otelslog.NewHandler(scope, otelslog.WithLoggerProvider(t.lp))
}

// ForceFlush exports all buffered telemetry.
func (t *Telemetry) ForceFlush(ctx context.Context) error {
	var errs []error
	for _, f := range t.flushers {
		errs = append(errs, f(ctx))
	}
	return errors.Join(errs...)
}

// Shutdown restores the previous globals (if Setup installed them), then
// flushes and stops every provider. Only the first call does work; later
// calls return the same result.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	t.once.Do(func() {
		if t.restore != nil {
			t.restore()
		}
		var errs []error
		for i := len(t.shutdowns) - 1; i >= 0; i-- {
			errs = append(errs, t.shutdowns[i](ctx))
		}
		t.shutdownErr = errors.Join(errs...)
	})
	return t.shutdownErr
}

func noopTelemetry() *Telemetry {
	return &Telemetry{
		tp:         tracenoop.NewTracerProvider(),
		mp:         metricnoop.NewMeterProvider(),
		lp:         lognoop.NewLoggerProvider(),
		propagator: propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}),
	}
}

// Setup builds the providers described by cfg and, unless cfg.NoGlobal is
// set, installs them as OpenTelemetry globals. It returns an error for invalid
// configuration, failing resource detection or exporter construction; in that
// case nothing is installed and nothing needs to be shut down.
//
// When cfg.Disabled is set (or OTEL_SDK_DISABLED=true) it returns a no-op
// Telemetry and installs nothing.
func Setup(ctx context.Context, cfg Config) (_ *Telemetry, err error) {
	disabled := cfg.Disabled
	if v, ok := os.LookupEnv("OTEL_SDK_DISABLED"); ok {
		b, perr := strconv.ParseBool(strings.TrimSpace(v))
		if perr != nil {
			return nil, fmt.Errorf("otelx/sdk: OTEL_SDK_DISABLED %q: %w", v, perr)
		}
		disabled = b
	}
	if disabled {
		return noopTelemetry(), nil
	}

	t := noopTelemetry()
	defer func() {
		if err != nil {
			_ = t.Shutdown(ctx) // release whatever was built before the failure
			err = fmt.Errorf("otelx/sdk: %w", err)
		}
	}()

	res, err := buildResource(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := t.buildTraces(ctx, cfg, res); err != nil {
		return nil, err
	}
	if err := t.buildMetrics(ctx, cfg, res); err != nil {
		return nil, err
	}
	if err := t.buildLogs(ctx, cfg, res); err != nil {
		return nil, err
	}
	if !cfg.NoGlobal {
		t.installGlobals(cfg.Logger)
	}
	return t, nil
}

func buildResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
	name := cfg.ServiceName
	if name == "" {
		name = "unknown_service:" + filepath.Base(os.Args[0])
	}
	attrs := []attribute.KeyValue{semconv.ServiceName(name)}
	if cfg.ServiceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(cfg.ServiceVersion))
	}
	if cfg.Environment != "" {
		attrs = append(attrs, semconv.DeploymentEnvironmentNameKey.String(cfg.Environment))
	}
	attrs = append(attrs, cfg.ResourceAttrs...)
	// Later options win: detectors override code attributes, env overrides all.
	res, err := resource.New(ctx,
		resource.WithSchemaURL(semconv.SchemaURL),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attrs...),
		resource.WithDetectors(cfg.ResourceDetectors...),
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, fmt.Errorf("resource: %w", err)
	}
	return res, nil
}

func (t *Telemetry) buildTraces(ctx context.Context, cfg Config, res *resource.Resource) error {
	def := ExporterOTLP
	if len(cfg.SpanExporters) > 0 {
		def = ExporterNone
	}
	exp, err := resolveExporter("TRACES", cfg.Traces, def)
	if err != nil {
		return err
	}
	smp, err := sampler(cfg)
	if err != nil {
		return err
	}
	exporters := cfg.SpanExporters
	if exp == ExporterOTLP {
		proto, err := resolveProtocol("TRACES", cfg.Traces)
		if err != nil {
			return err
		}
		e, err := spanExporter(ctx, proto, resolveParams("TRACES", cfg.Traces))
		if err != nil {
			return fmt.Errorf("traces exporter: %w", err)
		}
		exporters = append([]sdktrace.SpanExporter{e}, exporters...)
	}
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res), sdktrace.WithSampler(smp)}
	if exp == ExporterConsole {
		e, err := consoleSpanExporter()
		if err != nil {
			return fmt.Errorf("traces exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithSyncer(e))
	}
	for _, e := range exporters {
		opts = append(opts, sdktrace.WithBatcher(e))
	}
	// A provider without exporters still creates valid span contexts, so
	// trace ids keep flowing into logs and downstream requests.
	tp := sdktrace.NewTracerProvider(opts...)
	t.tp = tp
	t.flushers = append(t.flushers, tp.ForceFlush)
	t.shutdowns = append(t.shutdowns, tp.Shutdown)
	return nil
}

func (t *Telemetry) buildMetrics(ctx context.Context, cfg Config, res *resource.Resource) error {
	exp, err := resolveExporter("METRICS", cfg.Metrics, ExporterNone)
	if err != nil {
		return err
	}
	readers := cfg.MetricReaders
	if exp == ExporterOTLP {
		proto, err := resolveProtocol("METRICS", cfg.Metrics)
		if err != nil {
			return err
		}
		e, err := metricExporter(ctx, proto, resolveParams("METRICS", cfg.Metrics))
		if err != nil {
			return fmt.Errorf("metrics exporter: %w", err)
		}
		var ro []sdkmetric.PeriodicReaderOption
		if cfg.MetricInterval > 0 && !envSet("OTEL_METRIC_EXPORT_INTERVAL") {
			ro = append(ro, sdkmetric.WithInterval(cfg.MetricInterval))
		}
		readers = append([]sdkmetric.Reader{sdkmetric.NewPeriodicReader(e, ro...)}, readers...)
	}
	if exp == ExporterConsole {
		e, err := consoleMetricExporter()
		if err != nil {
			return fmt.Errorf("metrics exporter: %w", err)
		}
		var ro []sdkmetric.PeriodicReaderOption
		if cfg.MetricInterval > 0 && !envSet("OTEL_METRIC_EXPORT_INTERVAL") {
			ro = append(ro, sdkmetric.WithInterval(cfg.MetricInterval))
		}
		readers = append([]sdkmetric.Reader{sdkmetric.NewPeriodicReader(e, ro...)}, readers...)
	}
	if len(readers) == 0 {
		return nil
	}
	opts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	for _, r := range readers {
		opts = append(opts, sdkmetric.WithReader(r))
	}
	mp := sdkmetric.NewMeterProvider(opts...)
	t.mp = mp
	t.flushers = append(t.flushers, mp.ForceFlush)
	t.shutdowns = append(t.shutdowns, mp.Shutdown)
	if !cfg.NoRuntimeMetrics {
		// Callback-based: no goroutine; stops with the MeterProvider.
		if err := otelruntime.Start(otelruntime.WithMeterProvider(mp)); err != nil {
			return fmt.Errorf("runtime metrics: %w", err)
		}
	}
	return nil
}

func (t *Telemetry) buildLogs(ctx context.Context, cfg Config, res *resource.Resource) error {
	exp, err := resolveExporter("LOGS", cfg.Logs, ExporterNone)
	if err != nil {
		return err
	}
	procs := cfg.LogProcessors
	if exp == ExporterOTLP {
		proto, err := resolveProtocol("LOGS", cfg.Logs)
		if err != nil {
			return err
		}
		e, err := logExporter(ctx, proto, resolveParams("LOGS", cfg.Logs))
		if err != nil {
			return fmt.Errorf("logs exporter: %w", err)
		}
		procs = append([]sdklog.Processor{sdklog.NewBatchProcessor(e)}, procs...)
	}
	if exp == ExporterConsole {
		e, err := consoleLogExporter()
		if err != nil {
			return fmt.Errorf("logs exporter: %w", err)
		}
		procs = append([]sdklog.Processor{sdklog.NewSimpleProcessor(e)}, procs...)
	}
	if len(procs) == 0 {
		return nil
	}
	opts := []sdklog.LoggerProviderOption{sdklog.WithResource(res)}
	for _, p := range procs {
		opts = append(opts, sdklog.WithProcessor(p))
	}
	lp := sdklog.NewLoggerProvider(opts...)
	t.lp = lp
	t.flushers = append(t.flushers, lp.ForceFlush)
	t.shutdowns = append(t.shutdowns, lp.Shutdown)
	return nil
}

func (t *Telemetry) installGlobals(logger *log.Logger) {
	logger = log.OrDefault(logger)
	prevTP, prevMP, prevLP := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetLoggerProvider()
	prevProp, prevEH := otel.GetTextMapPropagator(), otel.GetErrorHandler()

	otel.SetTracerProvider(t.tp)
	otel.SetMeterProvider(t.mp)
	otel.SetLoggerProvider(t.lp)
	otel.SetTextMapPropagator(t.propagator)
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Error("opentelemetry error", log.Err(err))
	}))
	// OpenTelemetry's internal diagnostics (logr) go through the same logger;
	// logr verbosity V(n) maps to level -n, so the logger's level filters them.
	otel.SetLogger(logr.FromSlogHandler(logger.Handler()))

	t.restore = func() {
		otel.SetTracerProvider(prevTP)
		otel.SetMeterProvider(prevMP)
		otel.SetLoggerProvider(prevLP)
		otel.SetTextMapPropagator(prevProp)
		otel.SetErrorHandler(prevEH)
		// otel has no getter for its logger: fall back to the process logger.
		otel.SetLogger(logr.FromSlogHandler(log.Default().Handler()))
	}
}
