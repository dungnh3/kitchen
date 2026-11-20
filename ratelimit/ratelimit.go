// Package ratelimit limits request rates per key (subject or client IP)
// with GCRA buckets held in a Store: in memory by default, or shared across
// instances with the ratelimit/redis submodule.
//
// Policies are looked up by gRPC full method or HTTP route pattern; a
// Default policy applies one shared bucket per key to everything else.
// When a remote store fails, a circuit breaker stops calling it and each
// policy's OnStoreError decides: fall back to a per-instance in-memory
// limit (the default), allow, or deny.
//
// Denied requests get codes.ResourceExhausted with reason RATE_LIMITED and a
// google.rpc.RetryInfo; over HTTP that is 429 with Retry-After.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
)

// Errors returned to clients.
var (
	// ErrRateLimited is returned when a bucket is empty.
	ErrRateLimited = errs.New(codes.ResourceExhausted, "RATE_LIMITED",
		errs.Domain("ratelimit.kitchen"), errs.Message("rate limit exceeded"))
	// ErrUnavailable is returned when the store fails and the policy says Deny.
	ErrUnavailable = errs.New(codes.Unavailable, "RATE_LIMITER_UNAVAILABLE",
		errs.Domain("ratelimit.kitchen"), errs.Message("rate limiter unavailable"))
)

// Rule is a GCRA limit: Rate requests per Per, with bursts up to Burst.
type Rule struct {
	Rate  int
	Per   time.Duration
	Burst int // 0 means Rate
}

func (r Rule) normalize() Rule {
	if r.Burst == 0 {
		r.Burst = r.Rate
	}
	return r
}

func (r Rule) validate() error {
	switch {
	case r.Rate <= 0:
		return fmt.Errorf("rate must be > 0, got %d", r.Rate)
	case r.Per <= 0:
		return fmt.Errorf("per must be > 0, got %s", r.Per)
	case r.Burst < 0:
		return fmt.Errorf("burst must be >= 0, got %d", r.Burst)
	}
	return nil
}

// divide splits r across n instances, keeping at least 1.
func (r Rule) divide(n int) Rule {
	if n <= 1 {
		return r
	}
	r.Rate = max(1, r.Rate/n)
	r.Burst = max(1, r.Burst/n)
	return r
}

// StoreErrorMode says what to do while the store is failing.
type StoreErrorMode int

const (
	// FallbackLocal answers from an in-memory bucket whose rule is divided by
	// Config.Instances, so the fleet admits roughly the configured rate.
	FallbackLocal StoreErrorMode = iota
	// Allow lets every request through.
	Allow
	// Deny rejects every request with ErrUnavailable.
	Deny
)

// Policy is a Rule plus behavior.
type Policy struct {
	Rule
	OnStoreError StoreErrorMode
	// DryRun counts and reports limited requests but never blocks them.
	DryRun bool
}

// Decision is the outcome of one check.
type Decision struct {
	Allowed    bool
	Limit      int           // Rule.Rate of the bucket
	Remaining  int           // requests still allowed right now
	RetryAfter time.Duration // when denied: time until the next request is allowed
	ResetAfter time.Duration // time until the bucket is full again
	// Degraded is true when the decision did not come from the configured
	// store (fallback or OnStoreError).
	Degraded bool
	// Unavailable is true when the store failed and the policy said Deny.
	Unavailable bool
}

// Store holds bucket state. Allow consumes one request from the bucket key
// under rule r. An error means the store could not decide.
type Store interface {
	Allow(ctx context.Context, key string, r Rule) (Decision, error)
}

// Config configures a Limiter. Only Default or Policies is required.
type Config struct {
	// Store holds the buckets; nil means an in-memory store (per instance).
	Store Store
	// Default applies to every request without its own policy, with one
	// bucket per key shared by all of them. nil: such requests are not limited.
	Default *Policy
	// Policies by gRPC full method ("/pkg.Svc/Method") or HTTP route pattern
	// ("POST /v1/chat"). Each gets its own buckets.
	Policies map[string]Policy
	// Subject returns the authenticated subject of a request (for example
	// auth.SubjectFromContext). Requests with a subject are limited per
	// subject, the rest per client IP.
	Subject func(context.Context) (string, bool)
	// TrustedProxies are the proxies whose X-Forwarded-For is believed.
	TrustedProxies []netip.Prefix
	// Instances is the number of instances sharing the store, used to divide
	// rules in the FallbackLocal mode. Default 1.
	Instances int
	// StoreTimeout bounds each call to a non-local Store. Default 100ms.
	StoreTimeout time.Duration
	// BreakerCooldown is how long a failing store is skipped before one
	// probe request retries it. Default 1s.
	BreakerCooldown time.Duration
	// LocalMaxKeys bounds the in-memory store and the fallback (LRU).
	// Default 100_000.
	LocalMaxKeys int
	// Prefix starts every bucket key. Default "rl".
	Prefix string

	Logger        *slog.Logger
	MeterProvider metric.MeterProvider
}

const defaultScope = "*"

// Limiter enforces policies. It is safe for concurrent use and starts no
// goroutines.
type Limiter struct {
	store    Store
	remote   bool // store is not the in-memory one: use timeout + breaker
	fallback *LocalStore
	def      *Policy
	policies map[string]Policy
	subject  func(context.Context) (string, bool)
	trusted  []netip.Prefix

	instances int
	timeout   time.Duration
	cooldown  time.Duration
	prefix    string
	log       *slog.Logger
	now       func() time.Time

	openUntil atomic.Int64 // unix nanos; 0 = closed
	probing   atomic.Bool

	decisions   metric.Int64Counter
	storeErrors metric.Int64Counter
}

// New validates cfg and returns a Limiter.
func New(cfg Config) (*Limiter, error) {
	l := &Limiter{
		policies:  make(map[string]Policy, len(cfg.Policies)),
		subject:   cfg.Subject,
		trusted:   cfg.TrustedProxies,
		instances: max(1, cfg.Instances),
		timeout:   cmpOr(cfg.StoreTimeout, 100*time.Millisecond),
		cooldown:  cmpOr(cfg.BreakerCooldown, time.Second),
		prefix:    cfg.Prefix,
		log:       cfg.Logger,
		now:       time.Now,
	}
	if l.prefix == "" {
		l.prefix = "rl"
	}
	if l.log == nil {
		l.log = slog.Default()
	}
	var problems []error
	if cfg.Default != nil {
		if err := cfg.Default.validate(); err != nil {
			problems = append(problems, fmt.Errorf("default: %w", err))
		}
		p := *cfg.Default
		p.Rule = p.normalize()
		l.def = &p
	}
	for name, p := range cfg.Policies {
		if name == "" || name == defaultScope {
			problems = append(problems, fmt.Errorf("policy name %q is reserved", name))
			continue
		}
		if err := p.validate(); err != nil {
			problems = append(problems, fmt.Errorf("policy %q: %w", name, err))
		}
		p.Rule = p.normalize()
		l.policies[name] = p
	}
	if err := errors.Join(problems...); err != nil {
		return nil, fmt.Errorf("ratelimit: invalid config: %w", err)
	}

	maxKeys := cfg.LocalMaxKeys
	if maxKeys <= 0 {
		maxKeys = 100_000
	}
	if cfg.Store == nil {
		l.store = NewLocalStore(maxKeys)
	} else {
		l.store, l.remote = cfg.Store, true
		l.fallback = NewLocalStore(maxKeys)
	}

	mp := cfg.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter("github.com/dungnh3/kitchen/ratelimit")
	var err error
	if l.decisions, err = meter.Int64Counter("kitchen.ratelimit.decisions",
		metric.WithDescription("Rate-limit decisions by policy and result."),
		metric.WithUnit("{decision}")); err != nil {
		return nil, fmt.Errorf("ratelimit: metric: %w", err)
	}
	if l.storeErrors, err = meter.Int64Counter("kitchen.ratelimit.store.errors",
		metric.WithDescription("Failed calls to the rate-limit store."),
		metric.WithUnit("{error}")); err != nil {
		return nil, fmt.Errorf("ratelimit: metric: %w", err)
	}
	return l, nil
}

func cmpOr(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// policyFor returns the policy and bucket scope for name.
func (l *Limiter) policyFor(name string) (Policy, string, bool) {
	if p, ok := l.policies[name]; ok {
		return p, name, true
	}
	if l.def != nil {
		return *l.def, defaultScope, true
	}
	return Policy{}, "", false
}

// Limited reports whether requests for policy name are subject to any limit.
func (l *Limiter) Limited(name string) bool {
	_, _, ok := l.policyFor(name)
	return ok
}

// Allow checks one request for policy name (a method or route; the Default
// policy is used when it has none) and bucket key (for example
// "sub:<id>" or "ip:<addr>"). The Decision is always usable; a non-nil error
// reports a store failure that was handled according to OnStoreError.
func (l *Limiter) Allow(ctx context.Context, name, key string) (Decision, error) {
	p, scope, ok := l.policyFor(name)
	if !ok {
		return Decision{Allowed: true}, nil
	}
	bucket := l.prefix + ":" + scope + ":" + key
	d, err := l.check(ctx, p, bucket)
	result := "allowed"
	switch {
	case d.Unavailable:
		result = "unavailable"
	case !d.Allowed && p.DryRun:
		result = "dry_run"
		d.Allowed, d.RetryAfter = true, 0
	case !d.Allowed:
		result = "limited"
	case d.Degraded:
		result = "degraded"
	}
	l.decisions.Add(ctx, 1, metric.WithAttributes(
		attribute.String("policy", scope), attribute.String("result", result)))
	return d, err
}

func (l *Limiter) check(ctx context.Context, p Policy, bucket string) (Decision, error) {
	if !l.remote {
		return l.store.Allow(ctx, bucket, p.Rule)
	}
	open := l.openUntil.Load()
	if open != 0 {
		// Open, or half-open with another request already probing.
		if l.now().UnixNano() < open || !l.probing.CompareAndSwap(false, true) {
			return l.degrade(ctx, p, bucket, errStoreOpen)
		}
		defer l.probing.Store(false)
	}
	cctx, cancel := context.WithTimeout(ctx, l.timeout)
	d, err := l.store.Allow(cctx, bucket, p.Rule)
	cancel()
	if err != nil {
		// A caller that went away says nothing about the store's health.
		if ctx.Err() == nil {
			l.openUntil.Store(l.now().Add(l.cooldown).UnixNano())
		}
		l.storeErrors.Add(ctx, 1)
		return l.degrade(ctx, p, bucket, fmt.Errorf("ratelimit: store: %w", err))
	}
	if open != 0 {
		l.openUntil.Store(0) // probe succeeded
	}
	return d, nil
}

var errStoreOpen = errors.New("ratelimit: store circuit open")

func (l *Limiter) degrade(ctx context.Context, p Policy, bucket string, err error) (Decision, error) {
	switch p.OnStoreError {
	case Allow:
		return Decision{Allowed: true, Limit: p.Rate, Degraded: true}, err
	case Deny:
		return Decision{Limit: p.Rate, RetryAfter: l.cooldown, Degraded: true, Unavailable: true}, err
	default:
		d, _ := l.fallback.Allow(ctx, "fb:"+bucket, p.divide(l.instances))
		d.Degraded = true
		return d, err
	}
}
