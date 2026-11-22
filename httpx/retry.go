package httpx

import (
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"resty.dev/v3"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/timex"
)

// RetryPolicy retries failed requests with exponential backoff and jitter
// ([timex.Backoff]).
// Only idempotent methods (GET, HEAD, PUT, DELETE, OPTIONS, TRACE) are
// retried; use Request.IdempotencyKey to make a POST or PATCH retryable. Zero fields
// take the defaults.
type RetryPolicy struct {
	// MaxAttempts counts the first attempt. Default 3.
	MaxAttempts int
	// MinBackoff is the base delay. Default 100ms.
	MinBackoff time.Duration
	// MaxBackoff caps the delay. Default 5s.
	MaxBackoff time.Duration
	// MaxRetryAfter caps a server's Retry-After; with a longer one the
	// response is returned instead of waiting. Default 30s.
	MaxRetryAfter time.Duration
	// Retryable decides whether an attempt failed transiently. Default
	// DefaultRetryable.
	Retryable func(res *Response, err error) bool
}

func (p *RetryPolicy) normalize() {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}
	if p.MinBackoff <= 0 {
		p.MinBackoff = 100 * time.Millisecond
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 5 * time.Second
	}
	if p.MaxRetryAfter <= 0 {
		p.MaxRetryAfter = 30 * time.Second
	}
	if p.Retryable == nil {
		p.Retryable = DefaultRetryable
	}
}

// apply configures c with p. resty's own default conditions are replaced so
// that only p.Retryable decides.
func (p RetryPolicy) apply(c *resty.Client) {
	p.normalize()
	c.SetRetryCount(p.MaxAttempts - 1).
		SetRetryWaitTime(p.MinBackoff).
		SetRetryMaxWaitTime(p.MaxBackoff).
		SetRetryDefaultConditions(false).
		// One backoff formula across kitchen; resty still honours Retry-After
		// on 429 and 503 before asking this strategy.
		SetRetryDelayStrategy(func(res *resty.Response, _ error) (time.Duration, error) {
			return timex.Backoff(p.MinBackoff, p.MaxBackoff, res.Request.Attempt), nil
		}).
		AddRetryConditions(func(res *resty.Response, err error) bool {
			if res == nil || res.Request.Context().Err() != nil {
				return false
			}
			if res.RawResponse != nil {
				// resty waits for Retry-After on 429 and 503; refuse a wait
				// longer than the cap rather than block the caller.
				if d, ok := retryAfter(res.Header().Get("Retry-After")); ok && d > p.MaxRetryAfter {
					return false
				}
			}
			return p.Retryable(&Response{rr: res}, err)
		})
}

// DefaultRetryable retries network failures and timeouts of an attempt
// (as mapped by the client's transport) and status 429, 502, 503 and 504.
// It never retries a refused destination, a TLS verification failure or a
// cancelled request.
func DefaultRetryable(res *Response, err error) bool {
	if res.StatusCode() == 0 {
		switch errs.Code(err) {
		case codes.Unavailable, codes.DeadlineExceeded:
			return true // a refused destination is InvalidArgument
		}
		return false
	}
	switch res.StatusCode() {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func retryAfter(v string) (time.Duration, bool) {
	if v == "" {
		return 0, false
	}
	if s, err := strconv.Atoi(v); err == nil && s >= 0 {
		return time.Duration(s) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(time.Until(t), 0), true
	}
	return 0, false
}
