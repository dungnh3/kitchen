package httpx

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// RetryPolicy retries failed requests with exponential backoff and full
// jitter. A request is retried only when it is idempotent (GET, HEAD, PUT,
// DELETE, OPTIONS, TRACE, or any method with an Idempotency-Key header) and
// its body can be sent again. Zero fields take the defaults.
type RetryPolicy struct {
	// MaxAttempts counts the first attempt. Default 3.
	MaxAttempts int
	// MinBackoff is the base delay. Default 100ms.
	MinBackoff time.Duration
	// MaxBackoff caps the delay. Default 5s.
	MaxBackoff time.Duration
	// MaxRetryAfter caps a server's Retry-After; a longer one is not
	// waited for and the response is returned. Default 30s.
	MaxRetryAfter time.Duration
	// Retryable decides whether an attempt failed transiently. Default:
	// transport errors (except context cancellation) and status 429, 502,
	// 503, 504.
	Retryable func(resp *http.Response, err error) bool
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

// DefaultRetryable is the default RetryPolicy.Retryable.
func DefaultRetryable(resp *http.Response, err error) bool {
	if err != nil {
		return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func idempotent(r *Request) bool {
	switch r.method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodOptions, http.MethodTrace:
		return true
	}
	return r.header.Get("Idempotency-Key") != ""
}

func (c *Client) send(ctx context.Context, r *Request) (*http.Response, error) {
	attempts := 1
	if c.retry != nil && idempotent(r) && (r.body == nil || r.rewind) {
		attempts = c.retry.MaxAttempts
	}
	for n := 1; ; n++ {
		req, err := r.build(ctx)
		if err != nil {
			return nil, err
		}
		resp, err := c.hc.Do(req)
		if n >= attempts || !c.retry.Retryable(resp, err) {
			return resp, err
		}
		wait, ok := c.retry.delay(n, resp)
		if dl, has := ctx.Deadline(); !ok || has && time.Until(dl) < wait {
			return resp, err
		}
		if resp != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit))
			_ = resp.Body.Close()
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
	}
}

// delay returns the wait before attempt n+1 and false when a Retry-After is
// longer than MaxRetryAfter.
func (p *RetryPolicy) delay(n int, resp *http.Response) (time.Duration, bool) {
	if resp != nil {
		if ra, ok := retryAfter(resp.Header.Get("Retry-After")); ok {
			return ra, ra <= p.MaxRetryAfter
		}
	}
	ceiling := p.MinBackoff << min(n-1, 30)
	if ceiling <= 0 || ceiling > p.MaxBackoff {
		ceiling = p.MaxBackoff
	}
	return rand.N(ceiling) + 1, true
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
