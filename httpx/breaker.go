package httpx

import (
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
	// IsFailure classifies a response. Default: status 5xx. Transport
	// errors always count as failures.
	IsFailure func(*Response) bool
	// OnStateChange is called with "closed", "open" or "half-open".
	OnStateChange func(from, to string)
}

func (b BreakerConfig) build() resty.CircuitBreaker {
	var policies []resty.CircuitBreakerPolicy
	if b.IsFailure != nil {
		policies = append(policies, func(r *resty.Response) bool { return b.IsFailure(&Response{rr: r}) })
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
