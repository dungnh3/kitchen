package otelx

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc/stats"
)

// DefaultGRPCFilter reports whether an RPC should be instrumented. It skips
// the gRPC health and reflection services, which would otherwise dominate
// trace volume.
func DefaultGRPCFilter(info *stats.RPCTagInfo) bool {
	m := strings.TrimPrefix(info.FullMethodName, "/")
	return !strings.HasPrefix(m, "grpc.health.v1.Health/") &&
		!strings.HasPrefix(m, "grpc.reflection.")
}

// GRPCServerHandler returns an otelgrpc server stats handler that uses the
// global providers and [DefaultGRPCFilter]. Options are applied after the
// defaults, so otelgrpc.WithFilter replaces the filter.
//
//	grpc.NewServer(grpc.StatsHandler(otelx.GRPCServerHandler()))
func GRPCServerHandler(opts ...otelgrpc.Option) stats.Handler {
	return otelgrpc.NewServerHandler(append([]otelgrpc.Option{otelgrpc.WithFilter(DefaultGRPCFilter)}, opts...)...)
}

// GRPCClientHandler returns an otelgrpc client stats handler that uses the
// global providers and [DefaultGRPCFilter].
//
//	grpc.NewClient(target, grpc.WithStatsHandler(otelx.GRPCClientHandler()))
func GRPCClientHandler(opts ...otelgrpc.Option) stats.Handler {
	return otelgrpc.NewClientHandler(append([]otelgrpc.Option{otelgrpc.WithFilter(DefaultGRPCFilter)}, opts...)...)
}

// HTTPHandler wraps h with otelhttp server instrumentation. operation names
// the server span when no route is known.
func HTTPHandler(h http.Handler, operation string, opts ...otelhttp.Option) http.Handler {
	return otelhttp.NewHandler(h, operation, opts...)
}

// HTTPTransport wraps base (nil means http.DefaultTransport) with otelhttp
// client instrumentation, which also injects trace headers.
func HTTPTransport(base http.RoundTripper, opts ...otelhttp.Option) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return otelhttp.NewTransport(base, opts...)
}
