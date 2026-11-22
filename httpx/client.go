package httpx

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"resty.dev/v3"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/otelx"
)

// Defaults applied by New.
const (
	DefaultTimeout         = 30 * time.Second
	DefaultMaxResponseBody = 32 << 20
	DefaultMaxErrorBody    = 64 << 10
	DefaultMaxRedirects    = 10
	defaultIdlePerHost     = 100
)

// Client calls one HTTP API (or, without a base URL, any URL). It is safe
// for concurrent use: build it once with New and share it.
type Client struct {
	rc        *resty.Client
	base      *url.URL
	reqHooks  []RequestHook
	respHooks []ResponseHook
}

// RequestHook runs before a request is sent, after the per-call builder
// methods. It may inspect or change the request (for example add a
// signature header); an error aborts the call and is returned as is.
type RequestHook func(*Request) error

// ResponseHook runs once per call after the response arrived (after
// retries), whatever its status. An error is returned to the caller,
// joined with the call's own error if any.
type ResponseHook func(*Response) error

// RedirectPolicy decides whether to follow a redirect; it has the
// signature of http.Client.CheckRedirect.
type RedirectPolicy func(req *http.Request, via []*http.Request) error

// TransportSettings tunes the default transport. Zero fields keep the
// defaults (dial 30s, TLS handshake 10s, idle 90s, 100 idle connections in
// total and per host).
type TransportSettings struct {
	DialTimeout           time.Duration
	TLSHandshakeTimeout   time.Duration
	IdleConnTimeout       time.Duration
	ResponseHeaderTimeout time.Duration
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	MaxConnsPerHost       int
	// TLS replaces the TLS configuration (client certificates, roots).
	TLS *tls.Config
}

// Option configures New.
type Option func(*options)

type options struct {
	baseURL     string
	header      http.Header
	timeout     time.Duration
	transport   http.RoundTripper
	settings    TransportSettings
	middlewares []func(http.RoundTripper) http.RoundTripper
	logger      *log.Logger
	retry       *RetryPolicy
	breaker     *BreakerConfig
	maxBody     int64
	maxErrBody  int64
	mapper      ErrorMapper
	redirect    RedirectPolicy
	originGuard bool
	ssrf        *SSRFConfig
	reqHooks    []RequestHook
	respHooks   []ResponseHook
	otel        []otelhttp.Option
}

// WithBaseURL sets the base URL ("https://api.example.com/v1") request
// paths are joined to. With a base URL, OriginGuard refuses absolute
// request URLs on any other origin.
func WithBaseURL(u string) Option { return func(o *options) { o.baseURL = u } }

// WithHeader sets a header sent on every request.
func WithHeader(key, value string) Option { return func(o *options) { o.header.Set(key, value) } }

// WithBearer sends "Authorization: Bearer token" on every request.
func WithBearer(token string) Option { return WithHeader("Authorization", "Bearer "+token) }

// WithBasicAuth sends HTTP Basic credentials on every request.
func WithBasicAuth(user, password string) Option {
	return func(o *options) { o.header.Set("Authorization", basicAuth(user, password)) }
}

func basicAuth(user, password string) string {
	r := http.Request{Header: http.Header{}}
	r.SetBasicAuth(user, password)
	return r.Header.Get("Authorization")
}

// WithTimeout bounds each attempt, including reading the body. Default 30s;
// 0 disables it (rely on ctx deadlines, for example for streams).
func WithTimeout(d time.Duration) Option { return func(o *options) { o.timeout = d } }

// WithTransport replaces the base transport. It is still wrapped with the
// transport middlewares, upstream error mapping and otelx. It cannot be
// combined with WithSSRFGuard or WithTransportSettings: put SSRFControl in
// your own dialer instead.
func WithTransport(rt http.RoundTripper) Option { return func(o *options) { o.transport = rt } }

// WithTransportSettings tunes the default transport.
func WithTransportSettings(s TransportSettings) Option { return func(o *options) { o.settings = s } }

// WithTransportMiddleware wraps the base transport; the first middleware is
// innermost. Tracing and error mapping wrap all of them, so their work is
// traced and their errors are mapped.
func WithTransportMiddleware(mw ...func(http.RoundTripper) http.RoundTripper) Option {
	return func(o *options) { o.middlewares = append(o.middlewares, mw...) }
}

// WithRequestHook adds hooks run, in order, before every request (after
// OriginGuard).
func WithRequestHook(h ...RequestHook) Option {
	return func(o *options) { o.reqHooks = append(o.reqHooks, h...) }
}

// WithResponseHook adds hooks run, in order, after every response.
func WithResponseHook(h ...ResponseHook) Option {
	return func(o *options) { o.respHooks = append(o.respHooks, h...) }
}

// WithLogger receives the underlying client's warnings and errors.
// Default log.Default().
func WithLogger(l *log.Logger) Option { return func(o *options) { o.logger = l } }

// WithRetry enables retries; see RetryPolicy.
func WithRetry(p RetryPolicy) Option { return func(o *options) { o.retry = &p } }

// WithCircuitBreaker enables a circuit breaker; see BreakerConfig.
func WithCircuitBreaker(cfg BreakerConfig) Option { return func(o *options) { o.breaker = &cfg } }

// WithMaxResponseBody bounds every response body. Default 32 MiB; 0 removes
// the limit. Request.MaxResponseBody overrides it per call.
func WithMaxResponseBody(n int64) Option { return func(o *options) { o.maxBody = n } }

// WithMaxErrorBody bounds how much of a non-2xx body StatusError keeps.
// Default 64 KiB.
func WithMaxErrorBody(n int64) Option { return func(o *options) { o.maxErrBody = n } }

// WithErrorMapper replaces DefaultErrorMapper. A nil mapper returns the
// *StatusError itself.
func WithErrorMapper(m ErrorMapper) Option { return func(o *options) { o.mapper = m } }

// WithRedirectPolicy replaces SafeRedirectPolicy(DefaultMaxRedirects).
// NoRedirects never follows a redirect.
func WithRedirectPolicy(p RedirectPolicy) Option { return func(o *options) { o.redirect = p } }

// WithoutOriginGuard allows absolute request URLs on other origins even
// with a base URL. Client-level headers and credentials are then sent to
// them: only use it when every URL is trusted.
func WithoutOriginGuard() Option { return func(o *options) { o.originGuard = false } }

// WithSSRFGuard makes the default transport refuse non-public destination
// addresses (see SSRFConfig) and ignore proxy environment variables. Use it
// for clients that fetch URLs supplied by users.
func WithSSRFGuard(cfg SSRFConfig) Option { return func(o *options) { o.ssrf = &cfg } }

// WithTracerProvider sets the tracer provider. Default the global one.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *options) { o.otel = append(o.otel, otelhttp.WithTracerProvider(tp)) }
}

// WithMeterProvider sets the meter provider. Default the global one.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *options) { o.otel = append(o.otel, otelhttp.WithMeterProvider(mp)) }
}

// WithPropagator sets the trace propagator. Default the global one.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(o *options) { o.otel = append(o.otel, otelhttp.WithPropagators(p)) }
}

// New returns a Client configured with kitchen's defaults (see the package
// doc).
func New(opts ...Option) (*Client, error) {
	o := options{
		header:      http.Header{},
		timeout:     DefaultTimeout,
		maxBody:     DefaultMaxResponseBody,
		maxErrBody:  DefaultMaxErrorBody,
		mapper:      DefaultErrorMapper,
		redirect:    SafeRedirectPolicy(DefaultMaxRedirects),
		originGuard: true,
	}
	for _, opt := range opts {
		opt(&o)
	}
	c := &Client{}
	if o.baseURL != "" {
		b, err := url.Parse(o.baseURL)
		if err != nil {
			return nil, fmt.Errorf("httpx: base url: %w", err)
		}
		if b.Scheme != "http" && b.Scheme != "https" || b.Host == "" {
			return nil, fmt.Errorf("httpx: base url %q must be an absolute http(s) URL", o.baseURL)
		}
		c.base = b
	}
	if o.transport != nil && (o.ssrf != nil || o.settings != (TransportSettings{})) {
		return nil, errors.New("httpx: WithTransport cannot be combined with WithSSRFGuard or WithTransportSettings")
	}
	o.logger = log.OrDefault(o.logger)

	base := o.transport
	if base == nil {
		base = defaultTransport(o.settings, o.ssrf)
	}
	for _, mw := range o.middlewares {
		base = mw(base)
	}
	rc := resty.New()
	rc.SetTransport(otelx.HTTPTransport(upstreamTransport{base}, o.otel...))
	rc.SetCookieJar(nil) // a shared client must not replay one caller's cookies
	rc.SetLogger(restyLogger{o.logger})
	rc.SetTimeout(o.timeout)
	rc.SetResponseBodyLimit(o.maxBody)
	if o.redirect != nil {
		rc.SetRedirectPolicy(resty.RedirectPolicyFunc(o.redirect))
	}
	if c.base != nil {
		rc.SetBaseURL(o.baseURL)
	}
	rc.SetHeader("User-Agent", "kitchen-httpx")
	for k := range o.header {
		rc.SetHeader(k, o.header.Get(k))
	}
	// Capture the error body and build the error before auto-parse consumes
	// the body (ErrorInto targets still get decoded).
	rc.SetResponseMiddlewares(
		errorMiddleware(o.maxErrBody, o.mapper),
		resty.MiddlewareResponseAutoParse,
	)
	rc.OnError(redactErrorHook)
	rc.OnInvalid(redactErrorHook)
	if o.retry != nil {
		o.retry.apply(rc)
	}
	if o.breaker != nil {
		rc.SetCircuitBreaker(o.breaker.build())
	}
	if o.originGuard {
		c.reqHooks = append(c.reqHooks, OriginGuard)
	}
	c.reqHooks = append(c.reqHooks, o.reqHooks...)
	c.respHooks = o.respHooks
	c.rc = rc
	return c, nil
}

func defaultTransport(s TransportSettings, ssrf *SSRFConfig) *http.Transport {
	d := &net.Dialer{Timeout: cmpOr(s.DialTimeout, 30*time.Second), KeepAlive: 30 * time.Second}
	if ssrf != nil {
		d.Control = SSRFControl(*ssrf)
	}
	t := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           d.DialContext,
		ForceAttemptHTTP2:     true,
		DisableCompression:    true, // resty decompresses gzip/deflate itself
		TLSClientConfig:       s.TLS,
		TLSHandshakeTimeout:   cmpOr(s.TLSHandshakeTimeout, 10*time.Second),
		IdleConnTimeout:       cmpOr(s.IdleConnTimeout, 90*time.Second),
		ResponseHeaderTimeout: s.ResponseHeaderTimeout,
		ExpectContinueTimeout: time.Second,
		MaxIdleConns:          cmpOr(s.MaxIdleConns, defaultIdlePerHost),
		MaxIdleConnsPerHost:   cmpOr(s.MaxIdleConnsPerHost, defaultIdlePerHost),
		MaxConnsPerHost:       s.MaxConnsPerHost,
	}
	if ssrf != nil {
		t.Proxy = nil // a proxy would dial on our behalf, past the guard
	}
	return t
}

func cmpOr[T comparable](v, def T) T {
	var zero T
	if v == zero {
		return def
	}
	return v
}

// BaseURL returns the client's base URL, or "".
func (c *Client) BaseURL() string {
	if c.base == nil {
		return ""
	}
	return c.base.String()
}

// CloseIdleConnections closes idle connections of the client's transport.
func (c *Client) CloseIdleConnections() { c.rc.Client().CloseIdleConnections() }

// restyLogger routes resty's printf-style logs through kitchen/log, so they
// share the process log format. Messages are formatted only when the level
// is enabled.
type restyLogger struct{ l *log.Logger }

func (r restyLogger) logf(level log.Level, format string, v ...any) {
	ctx := context.Background()
	if !r.l.Enabled(ctx, level) {
		return
	}
	r.l.Log(ctx, level, "httpx: resty", log.String("detail", fmt.Sprintf(format, v...)))
}

func (r restyLogger) Errorf(format string, v ...any) { r.logf(log.LevelError, format, v...) }

func (r restyLogger) Warnf(format string, v ...any) { r.logf(log.LevelWarn, format, v...) }

func (r restyLogger) Debugf(format string, v ...any) { r.logf(log.LevelDebug, format, v...) }
