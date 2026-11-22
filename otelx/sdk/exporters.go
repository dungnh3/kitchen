package sdk

import (
	"context"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Console exporters (OTEL_*_EXPORTER=console) write to stdout.

func consoleSpanExporter() (sdktrace.SpanExporter, error) { return stdouttrace.New() }

func consoleMetricExporter() (sdkmetric.Exporter, error) { return stdoutmetric.New() }

func consoleLogExporter() (sdklog.Exporter, error) { return stdoutlog.New() }

// The OTLP exporter packages have identical option sets with distinct types;
// the helpers below map otlpParams onto each of them.

func spanExporter(ctx context.Context, proto Protocol, p otlpParams) (sdktrace.SpanExporter, error) {
	if proto == ProtocolHTTPProtobuf {
		var o []otlptracehttp.Option
		if p.endpoint != "" {
			if p.endpointURL {
				o = append(o, otlptracehttp.WithEndpointURL(p.endpoint))
			} else {
				o = append(o, otlptracehttp.WithEndpoint(p.endpoint))
			}
		}
		if p.headers != nil {
			o = append(o, otlptracehttp.WithHeaders(p.headers))
		}
		if p.insecure {
			o = append(o, otlptracehttp.WithInsecure())
		}
		if p.timeout > 0 {
			o = append(o, otlptracehttp.WithTimeout(p.timeout))
		}
		return otlptracehttp.New(ctx, o...)
	}
	var o []otlptracegrpc.Option
	if p.endpoint != "" {
		if p.endpointURL {
			o = append(o, otlptracegrpc.WithEndpointURL(p.endpoint))
		} else {
			o = append(o, otlptracegrpc.WithEndpoint(p.endpoint))
		}
	}
	if p.headers != nil {
		o = append(o, otlptracegrpc.WithHeaders(p.headers))
	}
	if p.insecure {
		o = append(o, otlptracegrpc.WithInsecure())
	}
	if p.timeout > 0 {
		o = append(o, otlptracegrpc.WithTimeout(p.timeout))
	}
	return otlptracegrpc.New(ctx, o...)
}

func metricExporter(ctx context.Context, proto Protocol, p otlpParams) (sdkmetric.Exporter, error) {
	if proto == ProtocolHTTPProtobuf {
		var o []otlpmetrichttp.Option
		if p.endpoint != "" {
			if p.endpointURL {
				o = append(o, otlpmetrichttp.WithEndpointURL(p.endpoint))
			} else {
				o = append(o, otlpmetrichttp.WithEndpoint(p.endpoint))
			}
		}
		if p.headers != nil {
			o = append(o, otlpmetrichttp.WithHeaders(p.headers))
		}
		if p.insecure {
			o = append(o, otlpmetrichttp.WithInsecure())
		}
		if p.timeout > 0 {
			o = append(o, otlpmetrichttp.WithTimeout(p.timeout))
		}
		return otlpmetrichttp.New(ctx, o...)
	}
	var o []otlpmetricgrpc.Option
	if p.endpoint != "" {
		if p.endpointURL {
			o = append(o, otlpmetricgrpc.WithEndpointURL(p.endpoint))
		} else {
			o = append(o, otlpmetricgrpc.WithEndpoint(p.endpoint))
		}
	}
	if p.headers != nil {
		o = append(o, otlpmetricgrpc.WithHeaders(p.headers))
	}
	if p.insecure {
		o = append(o, otlpmetricgrpc.WithInsecure())
	}
	if p.timeout > 0 {
		o = append(o, otlpmetricgrpc.WithTimeout(p.timeout))
	}
	return otlpmetricgrpc.New(ctx, o...)
}

func logExporter(ctx context.Context, proto Protocol, p otlpParams) (sdklog.Exporter, error) {
	if proto == ProtocolHTTPProtobuf {
		var o []otlploghttp.Option
		if p.endpoint != "" {
			if p.endpointURL {
				o = append(o, otlploghttp.WithEndpointURL(p.endpoint))
			} else {
				o = append(o, otlploghttp.WithEndpoint(p.endpoint))
			}
		}
		if p.headers != nil {
			o = append(o, otlploghttp.WithHeaders(p.headers))
		}
		if p.insecure {
			o = append(o, otlploghttp.WithInsecure())
		}
		if p.timeout > 0 {
			o = append(o, otlploghttp.WithTimeout(p.timeout))
		}
		return otlploghttp.New(ctx, o...)
	}
	var o []otlploggrpc.Option
	if p.endpoint != "" {
		if p.endpointURL {
			o = append(o, otlploggrpc.WithEndpointURL(p.endpoint))
		} else {
			o = append(o, otlploggrpc.WithEndpoint(p.endpoint))
		}
	}
	if p.headers != nil {
		o = append(o, otlploggrpc.WithHeaders(p.headers))
	}
	if p.insecure {
		o = append(o, otlploggrpc.WithInsecure())
	}
	if p.timeout > 0 {
		o = append(o, otlploggrpc.WithTimeout(p.timeout))
	}
	return otlploggrpc.New(ctx, o...)
}
