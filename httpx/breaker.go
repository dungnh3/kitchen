package httpx

import (
	"context"
	"errors"
	"net/http"
	"time"

	"resty.dev/v3"
)

// BreakerConfig configures the client's circuit breaker. A client talks to
// one upstream, so it has one breaker. While the breaker is open, calls
// fail at once with an errs.ErrDependencyUnavailable that wraps
// ErrCircuitOpen. After ResetTimeout one probe is let through (half-open).
// Zero fields take the defaults.
type BreakerConfig struct {
	// FailureThreshold opens the breaker after this many failures within
	// ResetTimeout. Default 5. Ignored when FailureRatio is set.
	FailureThreshold uint64
	// SuccessThreshold closes a half-open breaker after this many
	// successful probes. Default 1. Ignored when FailureRatio is set.
	SuccessThreshold uint64
	// FailureRatio (0..1], when set, opens the breaker when this share of
	// the calls within ResetTimeout failed, after MinRequests calls.
	FailureRatio float64
	// MinRequests is the minimum number of calls before FailureRatio
	// applies. Default 10.
	MinRequests uint64
	// ResetTimeout is both the sliding window and how long the breaker
	// stays open. Default 30s.
	ResetTimeout time.Duration
	// IsFailure classifies a response. Default: status 5xx. A transport
	// error (no response: connection refused, timeout, reset) always counts
	// as a failure, except a call cancelled by its caller and a destination
	// refused by the SSRF guard.
	IsFailure func(*Response) bool
	// OnStateChange is called with "closed", "open" or "half-open".
	OnStateChange func(from, to string)
}

// transportFailure is the marker response breakerTransport reports for a
// transport error; the first breaker policy classifies it as a failure.
var transportFailure = &resty.Response{}

func (b BreakerConfig) build() resty.CircuitBreaker {
	policies := []resty.CircuitBreakerPolicy{func(r *resty.Response) bool { return r == transportFailure }}
	if b.IsFailure != nil {
		policies = append(policies, func(r *resty.Response) bool { return b.IsFailure(&Response{rr: r}) })
	} else {
		policies = append(policies, resty.CircuitBreaker5xxPolicy)
	}
	reset := cmpOr(b.ResetTimeout, 30*time.Second)
	var obs resty.CircuitBreakerObserver
	var cb resty.CircuitBreaker
	if b.FailureRatio > 0 {
		r := resty.NewCircuitBreakerRatio(b.FailureRatio, cmpOr(b.MinRequests, uint64(10)), reset, policies...)
		cb, obs = r, r
	} else {
		c := resty.NewCircuitBreakerCount(cmpOr(b.FailureThreshold, uint64(5)), cmpOr(b.SuccessThreshold, uint64(1)), reset, policies...)
		cb, obs = c, c
	}
	if b.OnStateChange != nil {
		obs.OnStateChange(func(from, to resty.CircuitBreakerState) {
			b.OnStateChange(stateName(from), stateName(to))
		})
	}
	return cb
}

// breakerTransport reports every transport error to cb. resty only applies
// breaker policies to responses, so without it a down upstream (connection
// refused, timeouts) would never open the breaker.
type breakerTransport struct {
	next http.RoundTripper
	cb   resty.CircuitBreaker
}

func (t breakerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(r)
	if err != nil && countsAsUpstreamFailure(r, err) {
		t.cb.ApplyPolicies(transportFailure)
	}
	return resp, err
}

func (t breakerTransport) CloseIdleConnections() {
	if c, ok := t.next.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

// countsAsUpstreamFailure reports whether a transport error says something
// about the upstream. A caller's cancel and a destination refused by the
// SSRF guard do not.
func countsAsUpstreamFailure(r *http.Request, err error) bool {
	if errors.Is(err, ErrDestinationNotAllowed) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(r.Context().Err(), context.Canceled) {
		return false
	}
	return true
}

func stateName(s resty.CircuitBreakerState) string {
	switch s {
	case resty.CircuitBreakerStateOpen:
		return "open"
	case resty.CircuitBreakerStateHalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}
