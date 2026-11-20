package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultTimeout     = 30 * time.Second
	defaultMaxErrBody  = 64 << 10
	defaultIdlePerHost = 100
)

// Client calls one HTTP API. It is safe for concurrent use.
type Client struct {
	base       *url.URL
	hc         *http.Client
	header     http.Header
	retry      *RetryPolicy
	maxErrBody int64
}

// Option configures a Client.
type Option func(*options)

type options struct {
	timeout   time.Duration
	transport http.RoundTripper
	header    http.Header
	retry     *RetryPolicy
	maxErr    int64
	otel      []otelhttp.Option
}

// WithTimeout bounds each call, including reading the response body.
// Default 30s; 0 disables it (use ctx deadlines, for example for streams).
func WithTimeout(d time.Duration) Option { return func(o *options) { o.timeout = d } }

// WithTransport sets the base transport. It is wrapped with otelhttp.
// Default: a clone of http.DefaultTransport with 100 idle connections per
// host.
func WithTransport(rt http.RoundTripper) Option { return func(o *options) { o.transport = rt } }

// WithHeader sets a header sent on every request.
func WithHeader(key, value string) Option { return func(o *options) { o.header.Set(key, value) } }

// WithRetry enables retries; see RetryPolicy.
func WithRetry(p RetryPolicy) Option { return func(o *options) { o.retry = &p } }

// WithMaxErrorBody bounds how much of a non-2xx body StatusError keeps.
// Default 64 KiB.
func WithMaxErrorBody(n int64) Option { return func(o *options) { o.maxErr = n } }

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

// New returns a Client for the API at baseURL ("https://api.example.com/v1").
func New(baseURL string, opts ...Option) (*Client, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("httpx: base url: %w", err)
	}
	if base.Scheme != "http" && base.Scheme != "https" || base.Host == "" {
		return nil, fmt.Errorf("httpx: base url %q must be an absolute http(s) URL", baseURL)
	}
	o := options{timeout: defaultTimeout, header: http.Header{}, maxErr: defaultMaxErrBody}
	for _, opt := range opts {
		opt(&o)
	}
	if o.transport == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConnsPerHost = defaultIdlePerHost
		o.transport = t
	}
	if o.retry != nil {
		o.retry.normalize()
	}
	return &Client{
		base:       base,
		hc:         &http.Client{Timeout: o.timeout, Transport: otelhttp.NewTransport(o.transport, o.otel...)},
		header:     o.header,
		retry:      o.retry,
		maxErrBody: o.maxErr,
	}, nil
}

// Request is one call being built. It is not safe for concurrent use and
// must not be reused after Do or Send.
type Request struct {
	c      *Client
	method string
	path   string
	header http.Header
	query  url.Values
	body   func() (io.Reader, error)
	rewind bool // body can be produced again for a retry
	ctype  string
	err    error
}

// NewRequest starts a request. path is joined to the base URL path; it may
// carry a query string. An absolute URL replaces the base URL.
func (c *Client) NewRequest(method, path string) *Request {
	return &Request{c: c, method: method, path: path, header: http.Header{}, query: url.Values{}}
}

// Get starts a GET request.
func (c *Client) Get(path string) *Request { return c.NewRequest(http.MethodGet, path) }

// Post starts a POST request.
func (c *Client) Post(path string) *Request { return c.NewRequest(http.MethodPost, path) }

// Put starts a PUT request.
func (c *Client) Put(path string) *Request { return c.NewRequest(http.MethodPut, path) }

// Patch starts a PATCH request.
func (c *Client) Patch(path string) *Request { return c.NewRequest(http.MethodPatch, path) }

// Delete starts a DELETE request.
func (c *Client) Delete(path string) *Request { return c.NewRequest(http.MethodDelete, path) }

// Header sets a request header.
func (r *Request) Header(key, value string) *Request { r.header.Set(key, value); return r }

// Bearer sets "Authorization: Bearer token".
func (r *Request) Bearer(token string) *Request {
	return r.Header("Authorization", "Bearer "+token)
}

// IdempotencyKey sets Idempotency-Key, which also makes a POST or PATCH
// retryable.
func (r *Request) IdempotencyKey(key string) *Request { return r.Header("Idempotency-Key", key) }

// Query adds a query parameter.
func (r *Request) Query(key, value string) *Request { r.query.Add(key, value); return r }

// QueryValues adds query parameters.
func (r *Request) QueryValues(v url.Values) *Request {
	for k, vs := range v {
		r.query[k] = append(r.query[k], vs...)
	}
	return r
}

// JSON encodes v as the request body.
func (r *Request) JSON(v any) *Request {
	b, err := json.Marshal(v)
	if err != nil {
		r.err = fmt.Errorf("httpx: encode body: %w", err)
		return r
	}
	r.body = func() (io.Reader, error) { return bytes.NewReader(b), nil }
	r.rewind, r.ctype = true, "application/json"
	return r
}

// Body sends body with the content type. A *bytes.Reader, *bytes.Buffer or
// *strings.Reader body can be retried; any other reader cannot.
func (r *Request) Body(body io.Reader, contentType string) *Request {
	r.ctype = contentType
	switch b := body.(type) {
	case *bytes.Reader, *strings.Reader, *bytes.Buffer:
		data, err := io.ReadAll(b)
		if err != nil {
			r.err = err
			return r
		}
		r.body = func() (io.Reader, error) { return bytes.NewReader(data), nil }
		r.rewind = true
	default:
		used := false
		r.body = func() (io.Reader, error) {
			if used {
				return nil, errors.New("httpx: body already sent")
			}
			used = true
			return body, nil
		}
	}
	return r
}

// Multipart streams a multipart/form-data body written by fill. The body is
// not buffered, so the request is not retried.
func (r *Request) Multipart(fill func(*multipart.Writer) error) *Request {
	boundary := multipart.NewWriter(io.Discard).Boundary()
	r.ctype = "multipart/form-data; boundary=" + boundary
	r.body = func() (io.Reader, error) {
		pr, pw := io.Pipe()
		go func() {
			mw := multipart.NewWriter(pw)
			_ = mw.SetBoundary(boundary)
			err := fill(mw)
			if err == nil {
				err = mw.Close()
			}
			pw.CloseWithError(err)
		}()
		return pr, nil
	}
	r.rewind = false
	return r
}

func (r *Request) url() (string, error) {
	ref, err := url.Parse(r.path)
	if err != nil {
		return "", fmt.Errorf("httpx: path %q: %w", r.path, err)
	}
	var u *url.URL
	if ref.IsAbs() {
		u = ref
	} else {
		u = r.c.base.JoinPath(ref.Path)
		u.RawQuery = ref.RawQuery
	}
	if len(r.query) > 0 {
		q := u.Query()
		for k, vs := range r.query {
			q[k] = append(q[k], vs...)
		}
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}

func (r *Request) build(ctx context.Context) (*http.Request, error) {
	u, err := r.url()
	if err != nil {
		return nil, err
	}
	var body io.Reader
	if r.body != nil {
		if body, err = r.body(); err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, r.method, u, body)
	if err != nil {
		return nil, fmt.Errorf("httpx: %w", err)
	}
	for k, vs := range r.c.header {
		req.Header[k] = append([]string(nil), vs...)
	}
	for k, vs := range r.header {
		req.Header[k] = append([]string(nil), vs...)
	}
	if r.ctype != "" {
		req.Header.Set("Content-Type", r.ctype)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}
	return req, nil
}

// Send performs the request and returns the response whatever its status;
// the caller must close the body. Use it for streaming or non-JSON
// responses.
func (r *Request) Send(ctx context.Context) (*http.Response, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.c.send(ctx, r)
}

// Do performs the request. For a 2xx response it decodes a JSON body into
// out (if out is non-nil; a *[]byte receives the raw body). Any other status
// returns a *StatusError.
func (r *Request) Do(ctx context.Context, out any) error {
	resp, err := r.Send(ctx)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return r.c.statusError(resp)
	}
	switch o := out.(type) {
	case nil:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit))
		return nil
	case *[]byte:
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("httpx: read body: %w", err)
		}
		*o = b
		return nil
	default:
		if resp.StatusCode == http.StatusNoContent {
			return nil
		}
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("httpx: decode %s %s response: %w", r.method, resp.Request.URL.Redacted(), err)
		}
		return nil
	}
}

const drainLimit = 64 << 10

func (c *Client) statusError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, c.maxErrBody))
	return &StatusError{
		Method:     resp.Request.Method,
		URL:        resp.Request.URL.Redacted(),
		StatusCode: resp.StatusCode,
		Header:     resp.Header,
		Body:       body,
	}
}

// StatusError is returned by Do for a non-2xx response.
type StatusError struct {
	Method     string
	URL        string
	StatusCode int
	Header     http.Header
	// Body is the start of the response body (see WithMaxErrorBody).
	Body []byte
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("httpx: %s %s: %d %s", e.Method, e.URL, e.StatusCode, http.StatusText(e.StatusCode))
}

// Decode unmarshals the error body as JSON into v.
func (e *StatusError) Decode(v any) error { return json.Unmarshal(e.Body, v) }

// StatusCode returns the HTTP status of a *StatusError in err's chain, or 0.
func StatusCode(err error) int {
	if se, ok := errors.AsType[*StatusError](err); ok {
		return se.StatusCode
	}
	return 0
}
