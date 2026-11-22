package httpx

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"google.golang.org/grpc/codes"
	"resty.dev/v3"

	"github.com/dungnh3/kitchen/errs"
)

// ErrDestinationNotAllowed is returned when OriginGuard or the SSRF guard
// refuses a destination.
var ErrDestinationNotAllowed = errs.New(codes.InvalidArgument, "DESTINATION_NOT_ALLOWED",
	errs.Message("destination URL is not allowed"))

// StatusError describes a non-2xx response. It is the cause of the error
// returned by the client (see ErrorMapper); get it with
// errors.AsType[*httpx.StatusError](err).
type StatusError struct {
	Method string
	// URL is the final request URL with userinfo and query values redacted.
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

// ErrorMapper turns a non-2xx response into the error returned to the
// caller. It should wrap se so errors.AsType still finds it.
type ErrorMapper func(se *StatusError) error

// DefaultErrorMapper maps an upstream status to an errs error wrapping se,
// so a server built with kitchen answers its own clients with a sensible
// code without leaking the upstream body (the cause never leaves the
// process):
//
//   - 404 → errs.ErrNotFound
//   - 408, 504 → errs.ErrUpstreamTimeout (DeadlineExceeded)
//   - 429, 502, 503 → errs.ErrDependencyUnavailable (Unavailable), with the
//     upstream Retry-After as RetryInfo
//   - anything else, 401 and 403 included → errs.ErrInternal: an upstream
//     credential problem is ours, not the caller's.
func DefaultErrorMapper(se *StatusError) error {
	switch se.StatusCode {
	case http.StatusNotFound:
		return errs.Wrap(se, errs.ErrNotFound)
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return errs.Wrap(se, errs.ErrUpstreamTimeout)
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable:
		var opts []errs.Option
		if d, ok := retryAfter(se.Header.Get("Retry-After")); ok && d > 0 {
			opts = append(opts, errs.RetryAfter(d))
		}
		return errs.Wrap(se, errs.ErrDependencyUnavailable, opts...)
	default:
		return errs.Wrap(se, errs.ErrInternal)
	}
}

// errorMiddleware returns a response middleware that turns a 4xx/5xx
// response into an error: a *StatusError keeping up to maxBody bytes of the
// body, passed through mapper (nil returns it as is). The body stays
// readable for ErrorInto decoding and for the caller. New installs it first
// in the response chain.
func errorMiddleware(maxBody int64, mapper ErrorMapper) resty.ResponseMiddleware {
	return func(_ *resty.Client, res *resty.Response) error {
		raw := res.RawResponse
		if raw == nil || !res.IsStatusFailure() {
			return nil
		}
		se := &StatusError{StatusCode: raw.StatusCode, Header: raw.Header}
		if raw.Request != nil {
			se.Method = raw.Request.Method
			se.URL = RedactURL(raw.Request.URL)
		}
		if res.Body != nil && maxBody > 0 {
			b, err := io.ReadAll(io.LimitReader(res.Body, maxBody))
			if err == nil || len(b) > 0 {
				se.Body = b
			}
			res.Body = struct {
				io.Reader
				io.Closer
			}{io.MultiReader(bytes.NewReader(b), res.Body), res.Body}
		}
		if mapper == nil {
			return se
		}
		return mapper(se)
	}
}

// RedactURL returns u as a string with the password and every query value
// replaced, so API keys passed as ?key=... stay out of logs and errors.
func RedactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	c := *u
	if c.RawQuery != "" {
		q := c.Query()
		for k := range q {
			q[k] = []string{"REDACTED"}
		}
		c.RawQuery = q.Encode()
	}
	return c.Redacted()
}

// redactErrorHook rewrites the URL of a *url.Error in place so the error
// the caller logs carries no query values.
func redactErrorHook(_ *resty.Request, err error) {
	if ue, ok := errors.AsType[*url.Error](err); ok {
		if u, perr := url.Parse(ue.URL); perr == nil {
			ue.URL = RedactURL(u)
		}
	}
}

// upstreamTransport maps transport failures to errs errors: a timeout to
// errs.ErrUpstreamTimeout and other network failures to
// errs.ErrDependencyUnavailable. Cancellation, TLS verification failures
// and errs errors (such as a refused destination) pass through unchanged.
type upstreamTransport struct{ base http.RoundTripper }

func (t upstreamTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(r)
	if err == nil {
		return resp, nil
	}
	return resp, mapTransportError(err)
}

func (t upstreamTransport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

func mapTransportError(err error) error {
	if _, ok := errors.AsType[*errs.Error](err); ok {
		return err
	}
	if _, ok := errors.AsType[*tls.CertificateVerificationError](err); ok {
		return err
	}
	if _, ok := errors.AsType[x509.UnknownAuthorityError](err); ok {
		return err
	}
	if _, ok := errors.AsType[x509.HostnameError](err); ok {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return err
	case errors.Is(err, context.DeadlineExceeded):
		return errs.Wrap(err, errs.ErrUpstreamTimeout)
	}
	return errs.Wrap(err, errs.ErrDependencyUnavailable)
}
