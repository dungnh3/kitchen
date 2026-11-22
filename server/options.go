package server

import (
	"context"
	"net"
	"net/http"
	"time"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/i18n"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/ratelimit"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Option configures a Server.
type Option func(*config)

// RecoveryFunc turns a recovered panic into the error returned to the
// client. The panic is logged before it is called, and the result is
// converted by the server's errs.Handler.
type RecoveryFunc func(ctx context.Context, p any) error

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
	logger    *log.Logger
	errs      *errs.Handler
	i18n      *i18n.Bundle
	auth      Authenticator
	rateLimit *ratelimit.Limiter
	userID    func(context.Context) (string, bool)

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
	trustedProxies       *xcontext.TrustedProxies
	trustedIdentityPeers *xcontext.TrustedProxies
	trustAllIdentity     bool
	gatewayOpts          []runtime.ServeMuxOption
	forwardHeaders       []string
	httpErrorHandler     runtime.ErrorHandlerFunc
	streamingMarshalers  bool
	httpMiddlewares      []func(http.Handler) http.Handler
	cors                 *CORSConfig
	compression          bool
	httpServerConfig     func(*http.Server)
	checks               []Check
	version              string
	publicProbes         bool
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
		keepalivePolicy: keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true},
		maxMessageSize:  defaultMaxMessageSize,
		validation:      true,
		shutdownTimeout: defaultShutdownTimeout,
		publicProbes:    true,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// WithLogger sets the logger. Default: log.Default(), the process logger of
// package kitchen/log (installed by log.Setup in main). Records logged with a
// request context carry request_id and user_id (package kitchen/context
// stores them as log attributes).
func WithLogger(l *log.Logger) Option { return func(c *config) { c.logger = l } }

// WithServices mounts services.
func WithServices(s ...Service) Option {
	return func(c *config) { c.services = append(c.services, s...) }
}

// WithGRPCAddr sets the public gRPC listen address. Default ":10000". An
// empty address disables the public gRPC port: an HTTP-only server (for
// example HTTPRegistrar services such as an MCP endpoint) keeps the HTTP
// port, probes, drain, access log and tracing. The in-process gRPC server
// behind the REST gateway still runs. New fails when both ports are
// disabled.
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

// WithUnaryInterceptors appends unary interceptors (authentication,
// authorization, rate limiting). They run after recovery, request info,
// logging and error conversion, and before WithUserIDFrom and validation, so
// the errors they return are converted by the errs.Handler.
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
// errs.ErrInternal.
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
// report the client address in X-Forwarded-For. List every hop, Cloudflare
// ranges included (see xcontext.TrustedProxies). Default: none, the
// connection address is the client.
func WithTrustedProxies(p *xcontext.TrustedProxies) Option {
	return func(c *config) { c.trustedProxies = p }
}

// WithTrustedIdentityPeers sets the peers (internal services, an auth
// proxy) whose identity metadata (xcontext.IdentityHeaders) is accepted.
// From any other peer those keys are removed before any interceptor runs,
// and a Warn is logged (at most once a minute) so a missing service-to-
// service peer does not go unnoticed.
func WithTrustedIdentityPeers(p *xcontext.TrustedProxies) Option {
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
// metadata, in addition to xcontext.DefaultForwardHeaders. They are also captured in
// xcontext.Info.Headers.
func WithForwardHeaders(h ...string) Option {
	return func(c *config) { c.forwardHeaders = append(c.forwardHeaders, h...) }
}

// WithErrors sets the errs.Handler that converts every error the server
// returns: gRPC handler and interceptor errors, REST gateway errors, panics,
// oversized bodies. Configure its Localizer (package i18n) and Domain there.
// Default: errs.NewHandler with the server's logger, no domain, no
// localization.
func WithErrors(h *errs.Handler) Option { return func(c *config) { c.errs = h } }

// WithI18n localizes the errors of the default errs.Handler with b
// (google.rpc.LocalizedMessage in the language of the request). It cannot be
// combined with WithErrors: configure errs.Config.Localizer there instead.
func WithI18n(b *i18n.Bundle) Option { return func(c *config) { c.i18n = b } }

// WithUserIDFrom sets where the authenticated user id comes from, for
// example auth.SubjectFromContext. It runs after the user interceptors and
// HTTP middlewares (where authentication happens) and stores the id with
// xcontext.WithUserID, so handlers, logs (user_id) and outgoing
// calls (xcontext.AppendToOutgoing) see it.
func WithUserIDFrom(f func(context.Context) (string, bool)) Option {
	return func(c *config) { c.userID = f }
}

// WithHTTPErrorHandler replaces the REST gateway error handler entirely. By
// default the errs.Handler renders gateway errors (see WithErrors).
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

// WithCompression gzips HTTP responses (REST gateway and HTTPRegistrar
// handlers) of at least 1 KiB for clients that accept gzip. Server-Sent
// Events, NDJSON streams, already-compressed media and responses with a
// Content-Encoding are sent as is. Off by default: prefer compressing at the
// CDN, load balancer or ingress when one sits in front of the server (AWS
// ALB and ingress-nginx's defaults do not). gRPC uses its own per-call
// compression (grpc.UseCompressor).
func WithCompression() Option { return func(c *config) { c.compression = true } }

// WithHTTPServerConfig adjusts the http.Server before it starts (timeouts,
// TLS). Defaults: ReadHeaderTimeout 10s, IdleTimeout 650s (longer than the
// GCP load balancer's 600s), no read/write timeout (streaming).
func WithHTTPServerConfig(f func(*http.Server)) Option {
	return func(c *config) { c.httpServerConfig = f }
}

// WithReadiness adds readiness checks to /readyz (admin port, with failure
// details; public port, without). Results are cached for one second and
// concurrent probes share one run.
func WithReadiness(checks ...Check) Option {
	return func(c *config) { c.checks = append(c.checks, checks...) }
}

// WithVersion sets the version served at /version on the admin port.
// Default: the main module version from the build info.
func WithVersion(v string) Option { return func(c *config) { c.version = v } }

// WithoutPublicProbes stops serving /livez and /readyz on the HTTP port;
// probe the admin port or the grpc.health.v1 service instead.
func WithoutPublicProbes() Option { return func(c *config) { c.publicProbes = false } }

// infoOptions configures how request Info is built.
func (s *Server) infoOptions() xcontext.Options {
	return xcontext.Options{TrustedProxies: s.cfg.trustedProxies, Headers: s.cfg.forwardHeaders}
}
