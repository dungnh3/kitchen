package grpcx

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
)

// RetryPolicy configures gRPC's own retries, set through the default
// service config for every method of the target. gRPC replays the buffered
// request, so streams are retried too until the first response arrives.
// Retries are throttled per Conn with a budget of 10 tokens: each failure
// takes one, each success gives back 0.1, and below 5 tokens gRPC stops
// retrying. After about 5 failures in a row a down downstream receives no
// retry storm, and retries resume once calls succeed again. Zero fields
// take the defaults.
type RetryPolicy struct {
	// MaxAttempts counts the first attempt. Default 3; gRPC caps it at 5.
	MaxAttempts int
	// InitialBackoff is the first delay. Default 100ms. Each delay is
	// random in [0, current backoff].
	InitialBackoff time.Duration
	// MaxBackoff caps the backoff. Default 1s.
	MaxBackoff time.Duration
	// BackoffMultiplier grows the backoff after each attempt. Default 2.
	BackoffMultiplier float64
	// Codes are the retried codes. Default UNAVAILABLE only: it almost
	// always means the server did not process the request. Add others only
	// for idempotent methods.
	Codes []codes.Code
}

type jsonRetryPolicy struct {
	MaxAttempts          int      `json:"maxAttempts"`
	InitialBackoff       string   `json:"initialBackoff"`
	MaxBackoff           string   `json:"maxBackoff"`
	BackoffMultiplier    float64  `json:"backoffMultiplier"`
	RetryableStatusCodes []string `json:"retryableStatusCodes"`
}

type jsonServiceConfig struct {
	LoadBalancingConfig []map[string]struct{} `json:"loadBalancingConfig"`
	MethodConfig        []jsonMethodConfig    `json:"methodConfig,omitempty"`
	RetryThrottling     *jsonThrottling       `json:"retryThrottling,omitempty"`
}

type jsonMethodConfig struct {
	// Name [{}] applies the config to every method.
	Name        []struct{}      `json:"name"`
	RetryPolicy jsonRetryPolicy `json:"retryPolicy"`
}

type jsonThrottling struct {
	MaxTokens  int     `json:"maxTokens"`
	TokenRatio float64 `json:"tokenRatio"`
}

// serviceConfig returns the default service config: the load-balancing
// policy and, when p is not nil, the retry policy with throttling.
func serviceConfig(lb string, p *RetryPolicy) (string, error) {
	if lb == "" {
		return "", errors.New("grpcx: empty load-balancing policy")
	}
	sc := jsonServiceConfig{LoadBalancingConfig: []map[string]struct{}{{lb: {}}}}
	if p != nil {
		rp, err := p.json()
		if err != nil {
			return "", err
		}
		sc.MethodConfig = []jsonMethodConfig{{Name: []struct{}{{}}, RetryPolicy: rp}}
		sc.RetryThrottling = &jsonThrottling{MaxTokens: 10, TokenRatio: 0.1}
	}
	b, err := json.Marshal(sc)
	if err != nil {
		return "", fmt.Errorf("grpcx: service config: %w", err)
	}
	return string(b), nil
}

func (p RetryPolicy) json() (jsonRetryPolicy, error) {
	r := jsonRetryPolicy{
		MaxAttempts:       cmpOr(p.MaxAttempts, 3),
		InitialBackoff:    seconds(cmpOr(p.InitialBackoff, 100*time.Millisecond)),
		MaxBackoff:        seconds(cmpOr(p.MaxBackoff, time.Second)),
		BackoffMultiplier: cmpOr(p.BackoffMultiplier, 2),
	}
	if r.MaxAttempts < 2 || p.InitialBackoff < 0 || p.MaxBackoff < 0 || p.BackoffMultiplier < 0 {
		return r, errors.New("grpcx: RetryPolicy wants MaxAttempts >= 2 and no negative values; use WithoutRetry to disable retries")
	}
	cs := p.Codes
	if len(cs) == 0 {
		cs = []codes.Code{codes.Unavailable}
	}
	for _, c := range cs {
		if c == codes.OK {
			return r, errors.New("grpcx: RetryPolicy.Codes must not contain OK")
		}
		r.RetryableStatusCodes = append(r.RetryableStatusCodes, errs.CodeName(c))
	}
	return r, nil
}

func seconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', -1, 64) + "s"
}

func cmpOr[T comparable](v, def T) T {
	var zero T
	if v == zero {
		return def
	}
	return v
}
