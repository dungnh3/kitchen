package server

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/dungnh3/kitchen/reqctx"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// Option configures a Server.
type Option func(*config)

// RecoveryFunc turns a recovered panic into the error returned to the
// client. The panic and stack are logged before it is called.
type RecoveryFunc func(ctx context.Context, p any) error

// HTTPStatusFunc maps a gRPC status to an HTTP status code for REST error
// responses. Return 0 to keep the default mapping.
type HTTPStatusFunc func(*status.Status) int

// CORSConfig configures cross-origin requests on the HTTP port.
type CORSConfig struct {
	// AllowedOrigins may contain one "*" wildcard per entry
	// ("https://*.example.com"). Required.
	AllowedOrigins []string
	// AllowedMethods defaults to GET, POST, HEAD.
	AllowedMethods []string
	// AllowedHeaders defaults to Accept, Content-Type, X-Requested-With.
	AllowedHeaders   []string
	ExposedHeaders   []string
	AllowCredentials bool
	// MaxAge is the preflight cache duration; 0 lets the browser decide.
	MaxAge time.Duration
}

const (
	defaultGRPCAddr             = ":10000"
	defaultHTTPAddr             = ":8080"
	defaultMaxMessageSize       = 4 << 20
	defaultMaxConcurrentStreams = 1000
	defaultShutdownTimeout      = 25 * time.Second
)

type config struct {
	logger *slog.Logger

	grpcAddr, httpAddr   string
	grpcLis, httpLis     net.Listener
	adminAddr            string
	adminLis             net.Listener
	adminOpts            []AdminOption
	services             []Service
	tracerProvider       trace.TracerProvider
	meterProvider        metric.MeterProvider
	propagator           propagation.TextMapPropagator
	unary                []grpc.UnaryServerInterceptor
	stream               []grpc.StreamServerInterceptor
	grpcOpts             []grpc.ServerOption
	maxConcurrentStreams uint32
	keepaliveParams      keepalive.ServerParameters
	keepalivePolicy      keepalive.EnforcementPolicy
	maxMessageSize       int
	recovery             RecoveryFunc
	payloadLogging       bool
	reflection           bool
	validation           bool
	shutdownTimeout      time.Duration
	shutdownDelay        time.Duration
	trustedProxies       *reqctx.TrustedProxies
	trustedIdentityPeers *reqctx.TrustedProxies
	trustAllIdentity     bool
	gatewayOpts          []runtime.ServeMuxOption
	forwardHeaders       []string
	httpStatus           HTTPStatusFunc
	httpErrorHandler     runtime.ErrorHandlerFunc
	streamingMarshalers  bool
	httpMiddlewares      []func(http.Handler) http.Handler
	cors                 *CORSConfig
	httpServerConfig     func(*http.Server)
	checks               []Check
	version              string
	legacyHealthPaths    bool
}

func newConfig(opts []Option) *config {
	c := &config{
		grpcAddr:             defaultGRPCAddr,
		httpAddr:             defaultHTTPAddr,
		maxConcurrentStreams: defaultMaxConcurrentStreams,
		// Long-lived streams sit idle for minutes; without server pings an
		// L4 load balancer or sidecar drops them silently.
		keepaliveParams: keepalive.ServerParameters{Time: 60 * time.Second, Timeout: 20 * time.Second},
		// grpc-go's default MinTime (5m) answers clients that ping more often
		// with GOAWAY too_many_pings, killing every stream on the connection.
		keepalivePolicy:   keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true},
		maxMessageSize:    defaultMaxMessageSize,
		validation:        true,
		shutdownTimeout:   defaultShutdownTimeout,
		legacyHealthPaths: true,
	}
	for _, o := range opts {
		o(c)
	}
	if c.logger == nil {
		c.logger = slog.Default()
	}
	return c
}

// WithLogger sets the logger. Default slog.Default().
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// WithServices mounts services.
func WithServices(s ...Service) Option {
	return func(c *config) { c.services = append(c.services, s...) }
}

// WithGRPCAddr sets the gRPC listen address. Default ":10000".
func WithGRPCAddr(addr string) Option { return func(c *config) { c.grpcAddr = addr } }

// WithHTTPAddr sets the HTTP listen address. Default ":8080". An empty
// address disables the HTTP port (and the REST gateway).
func WithHTTPAddr(addr string) Option { return func(c *config) { c.httpAddr = addr } }

// WithGRPCListener serves gRPC on l instead of listening on the gRPC address.
// The server closes l.
func WithGRPCListener(l net.Listener) Option { return func(c *config) { c.grpcLis = l } }

// WithHTTPListener serves HTTP on l instead of listening on the HTTP address.
// The server closes l.
func WithHTTPListener(l net.Listener) Option { return func(c *config) { c.httpLis = l } }

// WithAdmin enables the admin port on addr (for example ":9090") with
// /livez, /readyz and the given admin options (pprof, metrics handler).
// Bind it to an address that is not exposed publicly.
func WithAdmin(addr string, opts ...AdminOption) Option {
	return func(c *config) {
		c.adminAddr = addr
		c.adminOpts = append(c.adminOpts, opts...)
	}
}

// WithAdminListener enables the admin port on l. The server closes l.
func WithAdminListener(l net.Listener, opts ...AdminOption) Option {
	return func(c *config) {
		c.adminLis = l
		c.adminOpts = append(c.adminOpts, opts...)
	}
}

// WithTracerProvider sets the tracer provider. Default otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *config) { c.tracerProvider = tp }
}

// WithMeterProvider sets the meter provider. Default otel.GetMeterProvider().
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *config) { c.meterProvider = mp }
}

// WithPropagator sets the trace context propagator. Default
// otel.GetTextMapPropagator().
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(c *config) { c.propagator = p }
}

// WithUnaryInterceptors appends unary interceptors. They run after recovery,
// request info and logging, and before validation.
func WithUnaryInterceptors(i ...grpc.UnaryServerInterceptor) Option {
	return func(c *config) { c.unary = append(c.unary, i...) }
}

// WithStreamInterceptors appends stream interceptors, in the same position
// as WithUnaryInterceptors.
func WithStreamInterceptors(i ...grpc.StreamServerInterceptor) Option {
	return func(c *config) { c.stream = append(c.stream, i...) }
}

// WithGRPCServerOptions appends raw grpc.ServerOptions, applied after the
// defaults so they override them.
func WithGRPCServerOptions(o ...grpc.ServerOption) Option {
	return func(c *config) { c.grpcOpts = append(c.grpcOpts, o...) }
}

// WithMaxConcurrentStreams sets the per-connection stream limit on the public
// gRPC port. Default 1000. The in-process gateway connection is not limited.
func WithMaxConcurrentStreams(n uint32) Option {
	return func(c *config) { c.maxConcurrentStreams = n }
}

// WithKeepalive overrides the keepalive parameters and enforcement policy.
// Default: ping idle connections every 60s (20s timeout) and accept client
// pings every 10s, even without streams.
func WithKeepalive(p keepalive.ServerParameters, e keepalive.EnforcementPolicy) Option {
	return func(c *config) { c.keepaliveParams, c.keepalivePolicy = p, e }
}

// WithMaxMessageSize sets the maximum gRPC message size, sent and received,
// on both gRPC servers and the gateway connection, and the REST gateway
// request body limit. Default 4 MiB.
func WithMaxMessageSize(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxMessageSize = n
		}
	}
}

// WithRecovery customises the error returned for a panic. Default
// codes.Internal "internal error".
func WithRecovery(f RecoveryFunc) Option { return func(c *config) { c.recovery = f } }

// WithPayloadLogging logs every unary request message at debug level.
// Off by default: payloads may contain secrets and personal data.
func WithPayloadLogging() Option { return func(c *config) { c.payloadLogging = true } }

// WithReflection registers gRPC server reflection on the public port.
func WithReflection() Option { return func(c *config) { c.reflection = true } }

// WithoutValidation disables the request validation interceptor
// (protovalidate rules and PGV Validate methods).
func WithoutValidation() Option { return func(c *config) { c.validation = false } }

// WithShutdownTimeout bounds the whole shutdown: delay, drain and
// Closer.Close. Default 25s; keep it below the pod's
// terminationGracePeriodSeconds.
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.shutdownTimeout = d
		}
	}
}

// WithShutdownDelay waits d after marking the server not ready and before
// draining, so load balancers stop routing to it first. It is part of the
// shutdown timeout. Default 0; around 5s suits Kubernetes.
func WithShutdownDelay(d time.Duration) Option { return func(c *config) { c.shutdownDelay = d } }

// WithTrustedProxies sets the proxies (load balancers, CDN) trusted to
// report the client address in X-Forwarded-For / CF-Connecting-IP. Default:
// none, the connection address is the client.
func WithTrustedProxies(p *reqctx.TrustedProxies) Option {
	return func(c *config) { c.trustedProxies = p }
}

// WithTrustedIdentityPeers sets the peers (internal services, an auth
// proxy) whose identity metadata (reqctx.IdentityHeaders) is accepted.
// From any other peer those keys are removed before any interceptor runs.
func WithTrustedIdentityPeers(p *reqctx.TrustedProxies) Option {
	return func(c *config) { c.trustedIdentityPeers = p }
}

// WithTrustAllIdentityHeaders accepts identity metadata from every peer.
// Use it only when neither port is reachable by untrusted clients.
func WithTrustAllIdentityHeaders() Option { return func(c *config) { c.trustAllIdentity = true } }

// WithGatewayOptions appends grpc-gateway ServeMux options, applied after
// the defaults so they override them (for example a marshaler).
func WithGatewayOptions(o ...runtime.ServeMuxOption) Option {
	return func(c *config) { c.gatewayOpts = append(c.gatewayOpts, o...) }
}

// WithForwardHeaders adds HTTP headers the gateway forwards to gRPC
// metadata, in addition to reqctx.ForwardHeaders.
func WithForwardHeaders(h ...string) Option {
	return func(c *config) { c.forwardHeaders = append(c.forwardHeaders, h...) }
}

// WithHTTPStatus maps gRPC statuses to HTTP status codes in REST error
// responses (for example from an error detail). The body stays a
// google.rpc.Status.
func WithHTTPStatus(f HTTPStatusFunc) Option { return func(c *config) { c.httpStatus = f } }

// WithHTTPErrorHandler replaces the REST error handler entirely.
func WithHTTPErrorHandler(h runtime.ErrorHandlerFunc) Option {
	return func(c *config) { c.httpErrorHandler = h }
}

// WithStreamingMarshalers serves server-streaming RPCs over REST as
// Server-Sent Events (Accept: text/event-stream) or newline-delimited JSON
// (Accept: application/x-ndjson).
func WithStreamingMarshalers() Option { return func(c *config) { c.streamingMarshalers = true } }

// WithHTTPMiddlewares wraps the HTTP handlers (gateway and HTTPRegistrar),
// inside the built-in middleware; the first one is outermost.
func WithHTTPMiddlewares(m ...func(http.Handler) http.Handler) Option {
	return func(c *config) { c.httpMiddlewares = append(c.httpMiddlewares, m...) }
}

// WithCORS enables CORS on the HTTP port. Off by default.
func WithCORS(cfg CORSConfig) Option { return func(c *config) { c.cors = &cfg } }

// WithHTTPServerConfig adjusts the http.Server before it starts (timeouts,
// TLS). Defaults: ReadHeaderTimeout 10s, IdleTimeout 650s (longer than the
// GCP load balancer's 600s), no read/write timeout (streaming).
func WithHTTPServerConfig(f func(*http.Server)) Option {
	return func(c *config) { c.httpServerConfig = f }
}

// WithReadiness adds readiness checks to /readyz and /ready.
func WithReadiness(checks ...Check) Option {
	return func(c *config) { c.checks = append(c.checks, checks...) }
}

// WithVersion sets the version served at /version. Default: the main
// module version from the build info.
func WithVersion(v string) Option { return func(c *config) { c.version = v } }

// WithoutLegacyHealthPaths stops serving /health, /ready, /api/status and
// /version on the HTTP port.
func WithoutLegacyHealthPaths() Option { return func(c *config) { c.legacyHealthPaths = false } }
