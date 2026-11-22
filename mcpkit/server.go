package mcpkit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Server is an MCP server. Create it with [New], register tools with
// [AddTool] (or other features via [Server.SDK]), then either mount
// [Server.Handler] on an HTTP mux or call [Server.RunStdio].
//
// A Server is safe for concurrent use.
type Server struct {
	sdk         *mcp.Server
	logger      *log.Logger
	toolTimeout time.Duration
	handler     http.Handler
	metaPath    string
	metaHandler http.Handler
}

// Option configures a [Server].
type Option func(*options)

type options struct {
	logger         *log.Logger
	errs           *errs.Handler
	instructions   string
	auth           *Auth
	stateful       bool
	sessionTimeout time.Duration
	maxBodyBytes   int64
	origins        []string
	toolTimeout    time.Duration
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	middleware     []mcp.Middleware
	serverOptions  []func(*mcp.ServerOptions)
}

// WithLogger sets the logger (also used by the MCP SDK). nil or default:
// log.Default().
func WithLogger(l *log.Logger) Option { return func(o *options) { o.logger = l } }

// WithErrors sets the errs handler that writes HTTP error responses mcpkit
// produces itself (an unavailable authorization server: 503). Pass the
// handler given to server.WithErrors so every route answers alike. Default:
// errs.NewHandler with the server's logger.
func WithErrors(h *errs.Handler) Option { return func(o *options) { o.errs = h } }

// WithInstructions sets the instructions sent to clients.
func WithInstructions(s string) Option { return func(o *options) { o.instructions = s } }

// WithAuth requires a bearer token on every HTTP request. See [Auth].
// It has no effect on [Server.Run] and [Server.RunStdio].
func WithAuth(a Auth) Option { return func(o *options) { o.auth = &a } }

// WithStateful switches the HTTP handler to stateful sessions
// (Mcp-Session-Id, GET notification stream, server-to-client requests on
// pre-2026-07-28 clients). Sessions idle for longer than idleTimeout are
// closed; zero keeps them until the client deletes them. Stateful sessions
// live in one process, so a load balancer must route by session.
//
// Default: stateless, every request independent, no affinity needed.
func WithStateful(idleTimeout time.Duration) Option {
	return func(o *options) { o.stateful, o.sessionTimeout = true, idleTimeout }
}

// WithMaxRequestBodyBytes limits HTTP request bodies (413 above it).
// Default: the SDK default, 4 MiB. Negative values are rejected by [New].
func WithMaxRequestBodyBytes(n int64) Option { return func(o *options) { o.maxBodyBytes = n } }

// WithAllowedOrigins allows browser clients served from these origins
// (scheme://host[:port]) to call the HTTP handler: they get CORS headers and
// pass cross-origin protection. Default: no cross-origin browser access;
// non-browser clients are unaffected.
func WithAllowedOrigins(origins ...string) Option {
	return func(o *options) { o.origins = append(o.origins, origins...) }
}

// WithToolTimeout sets the default deadline for every tool call. A tool can
// override it with [Timeout]. The deadline is applied to the handler's
// context; handlers must honour ctx for it to take effect. Default: none.
func WithToolTimeout(d time.Duration) Option { return func(o *options) { o.toolTimeout = d } }

// WithTracerProvider sets the tracer provider. Default: otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.tracerProvider = tp }
}

// WithMeterProvider sets the meter provider. Default: otel.GetMeterProvider().
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.meterProvider = mp }
}

// WithMiddleware adds SDK receiving middleware, run inside mcpkit's
// telemetry and panic recovery. The first middleware runs first.
func WithMiddleware(m ...mcp.Middleware) Option {
	return func(o *options) { o.middleware = append(o.middleware, m...) }
}

// WithServerOptions edits the SDK server options before the server is
// created (for example KeepAlive or CompletionHandler). mcpkit sets
// Instructions and Logger first; fn may override them.
func WithServerOptions(fn func(*mcp.ServerOptions)) Option {
	return func(o *options) { o.serverOptions = append(o.serverOptions, fn) }
}

// New creates a server. impl.Name is required.
func New(impl *mcp.Implementation, opts ...Option) (*Server, error) {
	if impl == nil || impl.Name == "" {
		return nil, errors.New("mcpkit: implementation name is required")
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.logger == nil {
		o.logger = log.Default()
	}
	if o.errs == nil {
		o.errs = errs.NewHandler(errs.Config{Logger: o.logger})
	}
	if o.tracerProvider == nil {
		o.tracerProvider = otel.GetTracerProvider()
	}
	if o.meterProvider == nil {
		o.meterProvider = otel.GetMeterProvider()
	}
	if o.maxBodyBytes < 0 {
		return nil, errors.New("mcpkit: max request body bytes must not be negative")
	}
	if o.toolTimeout < 0 || o.sessionTimeout < 0 {
		return nil, errors.New("mcpkit: timeouts must not be negative")
	}
	if o.auth != nil {
		if err := o.auth.validate(); err != nil {
			return nil, err
		}
	}

	sopts := &mcp.ServerOptions{Instructions: o.instructions, Logger: o.logger}
	for _, fn := range o.serverOptions {
		fn(sopts)
	}
	s := &Server{
		sdk:         mcp.NewServer(impl, sopts),
		logger:      o.logger,
		toolTimeout: o.toolTimeout,
	}

	tel, err := newTelemetry(o.tracerProvider, o.meterProvider)
	if err != nil {
		return nil, err
	}
	mw := append([]mcp.Middleware{tel.middleware, s.recoverMiddleware}, o.middleware...)
	s.sdk.AddReceivingMiddleware(mw...)

	if err := s.buildHandler(&o); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) buildHandler(o *options) error {
	var h http.Handler = mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return s.sdk },
		&mcp.StreamableHTTPOptions{
			Stateless:                    !o.stateful,
			SessionTimeout:               o.sessionTimeout,
			Logger:                       o.logger,
			MaxRequestBodyBytes:          o.maxBodyBytes,
			PropagateRequestCancellation: true,
		},
	)

	if a := o.auth; a != nil {
		ropts := &auth.RequireBearerTokenOptions{
			Scopes:                 a.Scopes,
			AllowMissingExpiration: a.AllowMissingExpiration,
			ClockSkew:              a.ClockSkew,
		}
		if a.Metadata != nil {
			p, _ := metadataPath(a.Metadata.Resource) // validated in New
			s.metaPath = p
			s.metaHandler = auth.ProtectedResourceMetadataHandler(a.Metadata)
			ropts.ResourceMetadataURL = originOf(a.Metadata.Resource) + p
		}
		// captureToken runs after verification, so the verified TokenInfo
		// (and principal) is in the context it reads.
		h = captureToken(h)
		h = auth.RequireBearerToken(a.Verifier, ropts)(h)
		h = withAuthErrors(o.errs, h)
	}

	cop := http.NewCrossOriginProtection()
	for _, origin := range o.origins {
		if err := cop.AddTrustedOrigin(origin); err != nil {
			return fmt.Errorf("mcpkit: allowed origin: %w", err)
		}
	}
	h = cop.Handler(h)
	s.handler = corsHandler(o.origins, h)
	return nil
}

// captureToken stores the raw bearer token in the request context so tool
// handlers can forward it upstream (see [TokenFrom]), and the kitchen
// principal set by [Verifier] (see auth.FromContext).
func captureToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w = originalWriter(w, r)
		ctx := withPrincipal(r.Context())
		if tok := bearerToken(r.Header.Get("Authorization")); tok != "" {
			ctx = withToken(ctx, tok)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// SDK returns the underlying SDK server, for features mcpkit does not wrap
// (resources, prompts, completions, notifications).
func (s *Server) SDK() *mcp.Server { return s.sdk }

// Handler returns the Streamable HTTP handler. Mount it on the MCP endpoint
// path, for example mux.Handle("/mcp", srv.Handler()).
func (s *Server) Handler() http.Handler { return s.handler }

// MetadataPath returns the RFC 9728 path where [Server.MetadataHandler]
// must be mounted, for example "/.well-known/oauth-protected-resource/mcp".
// It is "" when [Auth.Metadata] is not set.
func (s *Server) MetadataPath() string { return s.metaPath }

// MetadataHandler serves the protected-resource metadata document, or nil
// when [Auth.Metadata] is not set.
func (s *Server) MetadataHandler() http.Handler { return s.metaHandler }

// Run serves one client over transport t until ctx is cancelled or the
// client disconnects. If ts is not nil its token is made available to tool
// handlers through [TokenFrom]. It returns nil on cancellation.
func (s *Server) Run(ctx context.Context, t mcp.Transport, ts TokenSource) error {
	if ts != nil {
		tok, err := ts(ctx)
		if err != nil {
			return err
		}
		ctx = withToken(ctx, tok)
	}
	err := s.sdk.Run(ctx, t)
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return nil
	}
	return err
}

// RunStdio serves one client over stdin/stdout. Never log to stdout while
// it runs. ts supplies the upstream credential (see [EnvToken]); it may be
// nil.
func (s *Server) RunStdio(ctx context.Context, ts TokenSource) error {
	return s.Run(ctx, &mcp.StdioTransport{}, ts)
}

func originOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Scheme + "://" + u.Host
}
