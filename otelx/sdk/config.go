package sdk

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Exporter selects the built-in exporter for a signal.
type Exporter string

// Built-in exporters.
const (
	ExporterOTLP Exporter = "otlp"
	ExporterNone Exporter = "none"
)

// Protocol selects the OTLP transport.
type Protocol string

// OTLP protocols.
const (
	ProtocolGRPC         Protocol = "grpc"
	ProtocolHTTPProtobuf Protocol = "http/protobuf"
)

// SignalConfig configures the built-in OTLP exporter of one signal. Every
// field is overridden by the matching standard environment variable
// (OTEL_<SIGNAL>_EXPORTER, OTEL_EXPORTER_OTLP[_<SIGNAL>]_{PROTOCOL,ENDPOINT,
// HEADERS,INSECURE,TIMEOUT}) when that variable is set.
type SignalConfig struct {
	// Exporter is ExporterOTLP or ExporterNone. Empty means the signal's
	// default: traces use OTLP unless Config.SpanExporters is non-empty;
	// metrics and logs use none.
	Exporter Exporter
	// Protocol is ProtocolGRPC (default) or ProtocolHTTPProtobuf.
	Protocol Protocol
	// Endpoint is "host:port", or a full URL that is used as is (for HTTP it
	// must include the path, e.g. https://collector:4318/v1/traces). Empty
	// means the exporter default (localhost:4317 / localhost:4318).
	Endpoint string
	// Headers are sent with every export request, e.g. an Authorization
	// header for a vendor endpoint.
	Headers map[string]string
	// Insecure disables TLS for host:port endpoints.
	Insecure bool
	// Timeout is the per-export timeout. Zero means the exporter default.
	Timeout time.Duration
}

// Config configures [Setup]. The zero value exports traces over OTLP/gRPC to
// localhost:4317, samples every root span, and leaves metrics and logs
// disabled.
type Config struct {
	// Disabled makes Setup return a no-op Telemetry. OTEL_SDK_DISABLED
	// overrides it.
	Disabled bool
	// NoGlobal stops Setup from installing the providers, the propagator and
	// the error handler as OpenTelemetry globals.
	NoGlobal bool
	// Logger receives OpenTelemetry internal errors (export failures, ...).
	// nil means slog.Default().
	Logger *slog.Logger

	// ServiceName, ServiceVersion and Environment become the service.name,
	// service.version and deployment.environment.name resource attributes.
	// OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES override them.
	ServiceName    string
	ServiceVersion string
	Environment    string
	// ResourceAttrs are extra resource attributes, e.g. vendor-specific ones.
	ResourceAttrs []attribute.KeyValue
	// ResourceDetectors run at Setup; a failing detector makes Setup fail.
	ResourceDetectors []resource.Detector

	// Traces, Metrics and Logs configure the built-in OTLP exporters.
	Traces, Metrics, Logs SignalConfig

	// SampleRatio is the fraction of new root traces to sample, in [0,1].
	// nil means 1. OTEL_TRACES_SAMPLER(_ARG) override it.
	SampleRatio *float64
	// ExcludeOperations are dropped by the sampler in addition to
	// DefaultExcludedOperations (substring match on the span name).
	ExcludeOperations []string

	// SpanExporters are added, each behind a batch span processor.
	SpanExporters []sdktrace.SpanExporter
	// MetricReaders are added to the MeterProvider (for example a Prometheus
	// reader).
	MetricReaders []sdkmetric.Reader
	// LogProcessors are added to the LoggerProvider.
	LogProcessors []sdklog.Processor
	// MetricInterval is the export interval of the built-in OTLP metric
	// reader. Zero means the SDK default (OTEL_METRIC_EXPORT_INTERVAL or 60s).
	MetricInterval time.Duration
}

// envSet reports whether any of the environment variables is set.
func envSet(names ...string) bool {
	for _, n := range names {
		if _, ok := os.LookupEnv(n); ok {
			return true
		}
	}
	return false
}

// otlpEnv returns the generic and signal-specific variable names.
func otlpEnv(signal, suffix string) []string {
	return []string{"OTEL_EXPORTER_OTLP_" + suffix, "OTEL_EXPORTER_OTLP_" + signal + "_" + suffix}
}

func resolveExporter(signal string, sc SignalConfig, def Exporter) (Exporter, error) {
	e := sc.Exporter
	if v := strings.TrimSpace(os.Getenv("OTEL_" + signal + "_EXPORTER")); v != "" {
		e = Exporter(strings.ToLower(v))
	}
	if e == "" {
		e = def
	}
	switch e {
	case ExporterOTLP, ExporterNone:
		return e, nil
	}
	return "", fmt.Errorf("%s exporter %q is not supported (want otlp or none)", strings.ToLower(signal), e)
}

func resolveProtocol(signal string, sc SignalConfig) (Protocol, error) {
	p := sc.Protocol
	for _, n := range otlpEnv(signal, "PROTOCOL") { // signal-specific wins: checked last
		if v := strings.TrimSpace(os.Getenv(n)); v != "" {
			p = Protocol(strings.ToLower(v))
		}
	}
	switch p {
	case "", ProtocolGRPC:
		return ProtocolGRPC, nil
	case ProtocolHTTPProtobuf, "http":
		return ProtocolHTTPProtobuf, nil
	}
	return "", fmt.Errorf("%s OTLP protocol %q is not supported (want grpc or http/protobuf)", strings.ToLower(signal), p)
}

// otlpParams holds the code-level settings that survive env overrides. A zero
// field means "let the exporter read its environment or default".
type otlpParams struct {
	endpoint    string
	endpointURL bool
	headers     map[string]string
	insecure    bool
	timeout     time.Duration
}

func resolveParams(signal string, sc SignalConfig) otlpParams {
	var p otlpParams
	if sc.Endpoint != "" && !envSet(otlpEnv(signal, "ENDPOINT")...) {
		p.endpoint = sc.Endpoint
		p.endpointURL = strings.Contains(sc.Endpoint, "://")
	}
	if len(sc.Headers) > 0 && !envSet(otlpEnv(signal, "HEADERS")...) {
		p.headers = sc.Headers
	}
	if sc.Insecure && !envSet(otlpEnv(signal, "INSECURE")...) {
		p.insecure = true
	}
	if sc.Timeout > 0 && !envSet(otlpEnv(signal, "TIMEOUT")...) {
		p.timeout = sc.Timeout
	}
	return p
}
