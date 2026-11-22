package httpx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"resty.dev/v3"

	"github.com/dungnh3/kitchen/errs"
)

// Request is one call being built. It is not safe for concurrent use and
// must not be reused after Do or Send.
type Request struct {
	c      *Client
	rr     *resty.Request
	method string
	path   string
	err    error
}

// NewRequest starts a request. path is joined to the base URL; it may
// carry a query string and {name} placeholders filled by PathParam. Without
// a base URL (or with WithoutOriginGuard) it may be an absolute URL.
func (c *Client) NewRequest(method, path string) *Request {
	return &Request{c: c, rr: c.rc.R(), method: method, path: path}
}

// Get starts a GET request.
func (c *Client) Get(path string) *Request { return c.NewRequest(http.MethodGet, path) }

// Head starts a HEAD request.
func (c *Client) Head(path string) *Request { return c.NewRequest(http.MethodHead, path) }

// Post starts a POST request.
func (c *Client) Post(path string) *Request { return c.NewRequest(http.MethodPost, path) }

// Put starts a PUT request.
func (c *Client) Put(path string) *Request { return c.NewRequest(http.MethodPut, path) }

// Patch starts a PATCH request.
func (c *Client) Patch(path string) *Request { return c.NewRequest(http.MethodPatch, path) }

// Delete starts a DELETE request.
func (c *Client) Delete(path string) *Request { return c.NewRequest(http.MethodDelete, path) }

// Method returns the request method.
func (r *Request) Method() string { return r.method }

// Path returns the path or URL the request was started with, before path
// parameters are filled in.
func (r *Request) Path() string { return r.path }

// HeaderValue returns the request-level value of header key (client-level
// headers are merged when the request is sent).
func (r *Request) HeaderValue(key string) string { return r.rr.Header.Get(key) }

// Context returns the request context; it is set by Do or Send.
func (r *Request) Context() context.Context { return r.rr.Context() }

// Fail makes the call return err without sending anything. Request hooks
// may use it instead of returning an error.
func (r *Request) Fail(err error) *Request {
	if r.err == nil {
		r.err = err
	}
	return r
}

// Header sets a request header.
func (r *Request) Header(key, value string) *Request { r.rr.SetHeader(key, value); return r }

// Bearer sets "Authorization: Bearer token".
func (r *Request) Bearer(token string) *Request { return r.Header("Authorization", "Bearer "+token) }

// BasicAuth sets HTTP Basic credentials.
func (r *Request) BasicAuth(user, password string) *Request {
	return r.Header("Authorization", basicAuth(user, password))
}

// IdempotencyKey sets the Idempotency-Key header and lets the client's
// RetryPolicy retry the request whatever its method. The server must
// deduplicate on the key.
func (r *Request) IdempotencyKey(key string) *Request {
	r.rr.SetRetryAllowNonIdempotent(true)
	return r.Header("Idempotency-Key", key)
}

// Query adds a query parameter.
func (r *Request) Query(key, value string) *Request { r.rr.QueryParams.Add(key, value); return r }

// QueryValues adds query parameters.
func (r *Request) QueryValues(v url.Values) *Request {
	for k, vs := range v {
		for _, s := range vs {
			r.rr.QueryParams.Add(k, s)
		}
	}
	return r
}

// PathParam fills the {key} placeholder of the path with value, escaped so
// it cannot add path segments or a query.
func (r *Request) PathParam(key, value string) *Request { r.rr.SetPathParam(key, value); return r }

// JSON encodes v as the request body.
func (r *Request) JSON(v any) *Request {
	b, err := json.Marshal(v)
	if err != nil {
		return r.Fail(fmt.Errorf("httpx: encode body: %w", err))
	}
	r.rr.SetBody(b).SetContentType("application/json")
	return r
}

// Body sends body with the content type. A body that can be rewound
// (*bytes.Reader, *strings.Reader, *bytes.Buffer or any io.ReadSeeker) can
// be retried; any other reader disables retries for this request.
func (r *Request) Body(body io.Reader, contentType string) *Request {
	switch b := body.(type) {
	case *bytes.Buffer:
		r.rr.SetBody(bytes.NewReader(b.Bytes()))
	case io.ReadSeeker:
		r.rr.SetBody(b)
	default:
		r.rr.SetBody(body).SetRetryCount(0)
	}
	r.rr.SetContentType(contentType)
	return r
}

// Form sends v as an application/x-www-form-urlencoded body.
func (r *Request) Form(v url.Values) *Request { r.rr.SetFormDataFromValues(v); return r }

// FormField adds a text field to a multipart/form-data body.
func (r *Request) FormField(name, value string) *Request {
	r.rr.SetMultipartFormData(map[string]string{name: value})
	return r
}

// File adds a file part to a multipart/form-data body. The content type is
// detected from the content.
func (r *Request) File(field, fileName string, content io.Reader) *Request {
	r.rr.SetFileReader(field, fileName, content)
	return r
}

// MultipartField adds a part with an explicit content type to a
// multipart/form-data body.
func (r *Request) MultipartField(field, fileName, contentType string, content io.Reader) *Request {
	r.rr.SetMultipartField(field, fileName, contentType, content)
	return r
}

// Timeout bounds each attempt of this request, replacing WithTimeout.
func (r *Request) Timeout(d time.Duration) *Request { r.rr.SetTimeout(d); return r }

// MaxResponseBody bounds this response body, replacing WithMaxResponseBody;
// 0 removes the limit (for example for a large download with Send).
func (r *Request) MaxResponseBody(n int64) *Request { r.rr.SetResponseBodyLimit(n); return r }

// ErrorInto decodes a JSON error body (4xx/5xx) into v, in addition to the
// returned error.
func (r *Request) ErrorInto(v any) *Request { r.rr.SetResultError(v); return r }

// Do performs the request. For a 2xx response it decodes the JSON body
// into out when out is non-nil (a *[]byte receives the raw body; an empty
// body leaves out unchanged). A non-2xx response returns an error (see
// ErrorMapper); errors.AsType[*httpx.StatusError] gives its details.
func (r *Request) Do(ctx context.Context, out any) error {
	var raw *[]byte
	switch o := out.(type) {
	case nil:
	case *[]byte:
		raw = o
	default:
		r.rr.SetResult(out).SetResponseForceContentType("application/json")
	}
	res, err := r.execute(ctx)
	if err != nil {
		if res != nil && res.IsSuccess() && errors.Is(err, io.EOF) {
			return nil // empty 2xx body
		}
		return err
	}
	if raw != nil && res != nil {
		*raw = res.rr.Bytes()
	}
	return nil
}

// Send performs the request without reading the body, for streaming
// (SSE, downloads) or non-JSON responses: on success the caller must close
// Response.Body. A non-2xx response returns a nil Response and an error;
// the body prefix is in the *StatusError.
func (r *Request) Send(ctx context.Context) (*Response, error) {
	r.rr.SetResponseDoNotParse(true)
	res, err := r.execute(ctx)
	if err != nil {
		if res != nil {
			_ = res.Body().Close()
		}
		return nil, err
	}
	return res, nil
}

func (r *Request) execute(ctx context.Context) (*Response, error) {
	if ctx == nil {
		return nil, errors.New("httpx: nil context")
	}
	r.rr.SetContext(ctx)
	for _, h := range r.c.reqHooks {
		if r.err != nil {
			break
		}
		if err := h(r); err != nil {
			return nil, err
		}
	}
	if r.err != nil {
		return nil, r.err
	}
	rres, err := r.rr.Execute(r.method, r.path)
	if errors.Is(err, resty.ErrCircuitBreakerOpen) {
		err = errs.Wrap(fmt.Errorf("%w: %s %s", ErrCircuitOpen, r.method, redactString(r.path)), errs.ErrDependencyUnavailable)
	}
	var res *Response
	if rres != nil && rres.RawResponse != nil {
		res = &Response{rr: rres}
		for _, h := range r.c.respHooks {
			if herr := h(res); herr != nil {
				err = errors.Join(err, herr)
			}
		}
	}
	return res, err
}

// ErrCircuitOpen is in the chain of the error returned while the circuit
// breaker is open (an errs.ErrDependencyUnavailable).
var ErrCircuitOpen = errors.New("httpx: circuit breaker open")

// Response is a received HTTP response.
type Response struct{ rr *resty.Response }

// StatusCode returns the HTTP status, or 0 when no response was received.
func (r *Response) StatusCode() int {
	if r == nil || r.rr.RawResponse == nil {
		return 0
	}
	return r.rr.StatusCode()
}

// IsSuccess reports a 2xx status.
func (r *Response) IsSuccess() bool { c := r.StatusCode(); return c >= 200 && c <= 299 }

// Header returns the response headers.
func (r *Response) Header() http.Header {
	if r == nil || r.rr.RawResponse == nil {
		return http.Header{}
	}
	return r.rr.Header()
}

// Body returns the response body. With Send the caller must close it.
func (r *Response) Body() io.ReadCloser {
	if r == nil || r.rr.Body == nil {
		return http.NoBody
	}
	return r.rr.Body
}

// Duration returns the time from sending the request to receiving the
// response headers.
func (r *Response) Duration() time.Duration { return r.rr.Duration() }

// URL returns the final request URL (after redirects) with query values
// and passwords redacted.
func (r *Response) URL() string {
	if r == nil || r.rr.RawResponse == nil || r.rr.RawResponse.Request == nil {
		return ""
	}
	return RedactURL(r.rr.RawResponse.Request.URL)
}

func redactString(s string) string {
	if u, err := url.Parse(s); err == nil {
		return RedactURL(u)
	}
	if i := strings.IndexByte(s, '?'); i >= 0 {
		return s[:i]
	}
	return s
}
