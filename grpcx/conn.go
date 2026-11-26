package grpcx

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	xcontext "github.com/dungnh3/kitchen/context"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/otelx"
)

// Defaults applied by New.
const (
	// DefaultTimeout is the deadline of a unary call whose context has none.
	DefaultTimeout = 10 * time.Second
	// DefaultMaxMessageSize bounds sent and received messages, as kitchen's
	// server does.
	DefaultMaxMessageSize = 4 << 20
	// DefaultLoadBalancing is the load-balancing policy.
	DefaultLoadBalancing = "round_robin"
)

// DefaultKeepalive pings the server after 30s without activity while calls
// are active, and closes the connection when the ping is not answered
// within 10s. 30s is above the 10s minimum that kitchen's server enforces;
// no pings are sent without active calls, so servers with gRPC's stricter
// default policy do not reject them.
var DefaultKeepalive = keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}

// Conn is a client connection to one target. It implements
// grpc.ClientConnInterface, so generated stubs accept it. It is safe for
// concurrent use: build it once with New and share it.
type Conn struct {
	cc     *grpc.ClientConn
	target string
}

var _ grpc.ClientConnInterface = (*Conn)(nil)

// Option configures New.
type Option func(*options)

type options struct {
	tls         *tls.Config
	insecure    bool
	timeout     time.Duration
	retry       *RetryPolicy
	breaker     *BreakerConfig
	keepalive   keepalive.ClientParameters
	lb          string
	maxMsg      int
	tokens      TokenSource
	forwardAuth bool
	mapper      ErrorMapper
	unary       []grpc.UnaryClientInterceptor
	stream      []grpc.StreamClientInterceptor
	dialOpts    []grpc.DialOption
	logger      *log.Logger
	otel        []otelgrpc.Option
}

// WithTLSConfig replaces the TLS configuration (private roots, client
// certificates, server name). It is cloned; a zero MinVersion becomes TLS
// 1.2. It cannot be combined with WithInsecure.
func WithTLSConfig(c *tls.Config) Option { return func(o *options) { o.tls = c } }

// WithInsecure sends calls in plaintext. Use it only when something else
// encrypts the traffic (a service mesh doing mTLS) or for a local server.
// Credentials (WithTokenSource, WithForwardAuthorization) are refused with
// it.
func WithInsecure() Option { return func(o *options) { o.insecure = true } }

// WithTimeout sets the deadline of a unary call whose context has none.
// Default 10s; 0 disables it. Streams only follow their context.
func WithTimeout(d time.Duration) Option { return func(o *options) { o.timeout = d } }

// WithRetry replaces the default retry policy; zero fields take the
// defaults (see RetryPolicy).
func WithRetry(p RetryPolicy) Option { return func(o *options) { o.retry = &p } }

// WithoutRetry disables retries.
func WithoutRetry() Option { return func(o *options) { o.retry = nil } }

// WithCircuitBreaker enables a circuit breaker for the target; see
// BreakerConfig.
func WithCircuitBreaker(cfg BreakerConfig) Option { return func(o *options) { o.breaker = &cfg } }

// WithKeepalive replaces DefaultKeepalive. A Time below the server's
// enforcement minimum makes the server close the connection.
func WithKeepalive(p keepalive.ClientParameters) Option {
	return func(o *options) { o.keepalive = p }
}

// WithLoadBalancing sets the load-balancing policy, "round_robin" (default)
// or "pick_first".
func WithLoadBalancing(policy string) Option { return func(o *options) { o.lb = policy } }

// WithMaxMessageSize bounds sent and received messages. Default 4 MiB.
func WithMaxMessageSize(n int) Option { return func(o *options) { o.maxMsg = n } }

// WithTokenSource sends "authorization: Bearer <token>" on every call, with
// a token from ts. It requires TLS.
func WithTokenSource(ts TokenSource) Option { return func(o *options) { o.tokens = ts } }

// WithForwardAuthorization forwards the bearer token of the incoming call
// (the authorization metadata of ctx, a single "Bearer" value) to the
// downstream, unless the caller set an outgoing authorization itself. No
// other incoming metadata is forwarded. It requires TLS.
func WithForwardAuthorization() Option { return func(o *options) { o.forwardAuth = true } }

// WithErrorMapper replaces DefaultErrorMapper. A nil mapper returns the
// *StatusError itself, which errs.Handler sanitises to INTERNAL.
func WithErrorMapper(m ErrorMapper) Option {
	return func(o *options) { o.mapper = m }
}

// WithUnaryInterceptors adds unary interceptors. They run inside kitchen's
// (deadline, breaker, error mapping, propagation), so they see the raw
// gRPC errors.
func WithUnaryInterceptors(i ...grpc.UnaryClientInterceptor) Option {
	return func(o *options) { o.unary = append(o.unary, i...) }
}

// WithStreamInterceptors adds stream interceptors, inside kitchen's.
func WithStreamInterceptors(i ...grpc.StreamClientInterceptor) Option {
	return func(o *options) { o.stream = append(o.stream, i...) }
}

// WithDialOptions appends raw grpc.DialOptions, applied after kitchen's.
// Use it for what grpcx has no option for (a custom dialer, compression);
// it can override every default, so keep it rare.
func WithDialOptions(d ...grpc.DialOption) Option {
	return func(o *options) { o.dialOpts = append(o.dialOpts, d...) }
}

// WithLogger receives the circuit breaker's state changes. Default
// log.Default().
func WithLogger(l *log.Logger) Option { return func(o *options) { o.logger = l } }

// WithTracerProvider sets the tracer provider. Default the global one.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.otel = append(o.otel, otelgrpc.WithTracerProvider(tp)) }
}

// WithMeterProvider sets the meter provider. Default the global one.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.otel = append(o.otel, otelgrpc.WithMeterProvider(mp)) }
}

// WithPropagator sets the trace propagator. Default the global one.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.otel = append(o.otel, otelgrpc.WithPropagators(p)) }
}

// New returns a Conn to target, for example "dns:///billing:443" or
// "billing.internal:443" (the dns resolver is the default). It does not
// connect: the first call does.
func New(target string, opts ...Option) (*Conn, error) {
	o := options{
		timeout:   DefaultTimeout,
		retry:     &RetryPolicy{},
		keepalive: DefaultKeepalive,
		lb:        DefaultLoadBalancing,
		maxMsg:    DefaultMaxMessageSize,
		mapper:    DefaultErrorMapper,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if strings.TrimSpace(target) == "" {
		return nil, errors.New("grpcx: empty target")
	}
	if o.timeout < 0 || o.maxMsg <= 0 {
		return nil, errors.New("grpcx: WithTimeout must not be negative and WithMaxMessageSize must be positive")
	}
	if o.insecure && o.tls != nil {
		return nil, errors.New("grpcx: WithInsecure cannot be combined with WithTLSConfig")
	}
	if o.insecure && (o.tokens != nil || o.forwardAuth) {
		return nil, errors.New("grpcx: credentials (WithTokenSource, WithForwardAuthorization) require TLS; remove WithInsecure")
	}
	if o.tokens != nil && o.forwardAuth {
		return nil, errors.New("grpcx: WithTokenSource and WithForwardAuthorization are exclusive")
	}
	o.logger = log.OrDefault(o.logger)
	svcConfig, err := serviceConfig(o.lb, o.retry)
	if err != nil {
		return nil, err
	}

	var creds credentials.TransportCredentials
	if o.insecure {
		creds = insecure.NewCredentials()
	} else {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if o.tls != nil {
			cfg = o.tls.Clone()
			if cfg.MinVersion == 0 {
				cfg.MinVersion = tls.VersionTLS12
			}
		}
		creds = credentials.NewTLS(cfg)
	}

	unary := []grpc.UnaryClientInterceptor{}
	stream := []grpc.StreamClientInterceptor{}
	if o.timeout > 0 {
		unary = append(unary, deadlineInterceptor(o.timeout))
	}
	if o.breaker != nil {
		b := newBreaker(target, *o.breaker, o.logger)
		unary = append(unary, b.unary)
		stream = append(stream, b.stream)
	}
	unary = append(unary, errorUnary(o.mapper))
	stream = append(stream, errorStream(o.mapper))
	unary = append(unary, xcontext.UnaryClientInterceptor())
	stream = append(stream, xcontext.StreamClientInterceptor())
	if o.forwardAuth {
		unary = append(unary, forwardUnary)
		stream = append(stream, forwardStream)
	}
	unary = append(unary, o.unary...)
	stream = append(stream, o.stream...)

	dial := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(svcConfig),
		grpc.WithKeepaliveParams(o.keepalive),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(o.maxMsg), grpc.MaxCallSendMsgSize(o.maxMsg)),
		grpc.WithStatsHandler(otelx.GRPCClientHandler(o.otel...)),
		grpc.WithChainUnaryInterceptor(unary...),
		grpc.WithChainStreamInterceptor(stream...),
		grpc.WithUserAgent("kitchen-grpcx"),
	}
	if o.tokens != nil {
		dial = append(dial, grpc.WithPerRPCCredentials(tokenCreds{o.tokens}))
	}
	dial = append(dial, o.dialOpts...)
	cc, err := grpc.NewClient(target, dial...)
	if err != nil {
		return nil, fmt.Errorf("grpcx: %w", err)
	}
	return &Conn{cc: cc, target: target}, nil
}

// Invoke performs a unary RPC; generated stubs call it.
func (c *Conn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	return c.cc.Invoke(ctx, method, args, reply, opts...)
}

// NewStream begins a streaming RPC; generated stubs call it.
func (c *Conn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.cc.NewStream(ctx, desc, method, opts...)
}

// Target returns the target given to New.
func (c *Conn) Target() string { return c.target }

// State returns the connectivity state, for diagnostics. It does not make
// the connection; readiness checks should not depend on it.
func (c *Conn) State() connectivity.State { return c.cc.GetState() }

// Close closes the connection; calls in progress fail with Canceled. ctx is
// accepted for the kitchen Close(ctx) convention; closing does not block.
func (c *Conn) Close(_ context.Context) error {
	if err := c.cc.Close(); err != nil {
		return fmt.Errorf("grpcx: close %s: %w", c.target, err)
	}
	return nil
}

// deadlineInterceptor gives a unary call without a deadline one of d.
func deadlineInterceptor(d time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, d)
			defer cancel()
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
