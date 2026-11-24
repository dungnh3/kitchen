// Package ratelimit limits request rates with GCRA buckets held in a Store:
// in memory by default, or shared across instances with the ratelimit/redis
// submodule.
//
// Every request resolves to its most trusted identity (its tier): a verified
// user, else a verified anonymous subject, else an unverified device id, else
// its client IP. A Policy holds one or more Limits, each counting one kind of
// identity (By); a request must pass every Limit that applies to it. For
// example DefaultLimits gives users, anonymous subjects and devices 20/s each
// and unauthenticated IPs a looser 300/s, so rotating fake device ids does not
// help and users behind a shared NAT are not punished.
//
// Policies are looked up by gRPC full method or HTTP route pattern; a Default
// policy applies, with buckets shared across endpoints, to everything else.
// Exempt skips trusted identities or networks, Overrides changes the rule for
// one identity, Skip leaves endpoints unlimited. When a remote store fails, a
// circuit breaker stops calling it and each policy's OnStoreError decides:
// fall back to a per-instance in-memory limit (the default), allow, or deny.
//
// Denied requests get codes.ResourceExhausted with reason RATE_LIMITED and a
// google.rpc.RetryInfo; over HTTP that is 429 with Retry-After. Responses
// carry RateLimit-Policy / RateLimit (IETF draft) and X-RateLimit-* headers.
//
// By default the user is xcontext.UserID (set by package auth), the device id
// is xcontext.Info.DeviceID and the client IP is the one package server
// resolved with its trusted proxies (xcontext.Info.ClientIP). With package
// server, install the limiter with server.WithRateLimit.
package ratelimit

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dungnh3/kitchen/log"

	"github.com/dungnh3/kitchen/errs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
)

// Errors returned to clients.
var (
	// ErrRateLimited is returned when a bucket is empty (the errs catalog
	// sentinel; the domain is the service's, from its errs.Handler).
	ErrRateLimited = errs.ErrRateLimited
	// ErrUnavailable is returned when the store fails and the policy says Deny.
	ErrUnavailable = errs.New(codes.Unavailable, "RATE_LIMITER_UNAVAILABLE",
		errs.Message("rate limiter unavailable"))
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
	case int64(r.Per/time.Microsecond) < int64(r.Rate):
		return fmt.Errorf("rate %d per %s is over one request per microsecond", r.Rate, r.Per)
	}
	// The bucket state must stay exact in a Lua (float64) number.
	if interval, _ := gcraParams(r); r.Burst > 0 && interval > maxTolerance/int64(r.Burst) {
		return fmt.Errorf("burst %d x %s is too long", r.Burst, r.Per/time.Duration(r.Rate))
	}
	return nil
}

// maxTolerance bounds the GCRA burst tolerance (microseconds) so the
// theoretical arrival time stays an exact integer in float64 (2^53).
const maxTolerance = 1 << 52

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

// Limit is one rule counted against one kind of identity.
type Limit struct {
	Rule
	By By
	// Key, when set, replaces By: it returns the bucket key of a request (for
	// example a tenant id), or false when the limit does not apply.
	Key func(context.Context) (string, bool)
}

// rank orders limits from the most specific identity to the most shared.
func (lim Limit) rank() int {
	if lim.Key != nil {
		return 0
	}
	switch lim.By {
	case ByIP, byCallerDeviceIP:
		return 1
	case ByAnyIP:
		return 2
	case ByGlobal:
		return 3
	}
	return 0
}

// DefaultLimits is a starting point to validate with dry-run metrics, not
// tuned values: 20/s (bursts of 40) per user, anonymous subject and device,
// and 300/s (bursts of 600) per client IP for unauthenticated requests,
// loose because many clients share one IP behind carrier NAT.
func DefaultLimits() []Limit {
	per := Rule{Rate: 20, Per: time.Second, Burst: 40}
	return []Limit{
		{Rule: per, By: ByUser},
		{Rule: per, By: ByAnon},
		{Rule: per, By: ByDevice},
		{Rule: Rule{Rate: 300, Per: time.Second, Burst: 600}, By: ByIP},
	}
}

// Policy is a set of limits plus behavior. A request must pass every limit
// that applies to it. Limits are checked from the most specific identity
// (user, anon, device, custom key) to the most shared (IP, any IP, global),
// and checking stops at the first denial, so a denied request does not use
// up the shared buckets.
type Policy struct {
	// Rule is shorthand for one Limit{Rule: Rule, By: ByCaller}. Leave it zero
	// when Limits is set (it is then added to them when non-zero).
	Rule
	// Limits are the limits of the policy.
	Limits []Limit
	// OnStoreError says what to do while a remote store fails.
	OnStoreError StoreErrorMode
	// DryRun counts and reports limited requests but never blocks them.
	DryRun bool
}

// Decision is the outcome of one check. For a policy with several limits it
// is the denying limit with the longest RetryAfter, or else the limit with
// the fewest remaining requests.
type Decision struct {
	Allowed    bool
	Limit      int           // Rule.Rate of the deciding bucket
	Remaining  int           // requests still allowed right now
	RetryAfter time.Duration // when denied: time until the next request is allowed
	ResetAfter time.Duration // time until the bucket is full again
	// Degraded is true when a bucket was not answered by the configured
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

// Event describes one decision, for Config.OnDecision.
type Event struct {
	Policy string // gRPC method or HTTP route, "default" for the Default policy
	Limit  string // the deciding limit: "caller", "user", "ip", ..., "custom"
	Result string // allowed, limited, dry_run, degraded, unavailable
	Decision
}

// Config configures a Limiter. Only Default or Policies is required.
type Config struct {
	// Store holds the buckets; nil means an in-memory store (per instance).
	Store Store
	// Default applies to every request without its own policy, with one set
	// of buckets per key shared by all of them. nil: such requests are not
	// limited. &Policy{Limits: DefaultLimits()} is a reasonable start.
	Default *Policy
	// Policies by gRPC full method ("/pkg.Svc/Method") or HTTP route pattern
	// ("POST /v1/chat"). Each gets its own buckets.
	Policies map[string]Policy
	// Skip lists methods or routes that are never limited, even by Default.
	Skip []string

	// Subject returns the verified user of a request. Default: xcontext.UserID
	// (set by package auth).
	Subject func(context.Context) (string, bool)
	// Anonymous returns the subject of a verified anonymous token (checked
	// before Subject). Default: none.
	Anonymous func(context.Context) (string, bool)
	// Device returns the unverified device id. Default: xcontext.Info.DeviceID
	// (X-Device-Id). Device ids are hashed in bucket keys.
	Device func(context.Context) (string, bool)

	// Exempt lists identities that are never limited: "user:<id>",
	// "anon:<id>", "ip:<address>" (that exact address) or "ip:<CIDR>" (for
	// example internal networks). Device ids are client-controlled and are
	// rejected here.
	Exempt []string
	// Overrides change the rule for one identity ("user:<id>", "anon:<id>",
	// "device:<id>", "ip:<address>") in every limit that counts it; the bucket
	// stays the same, so an IPv6 override applies to the address's /64 bucket.
	// For example a higher limit for a partner account. A device override
	// does not lift the IP limits that also count unverified devices.
	Overrides map[string]Rule

	// DryRun reports every policy as dry-run.
	DryRun bool
	// Disabled turns the limiter off (everything is allowed, nothing counted).
	Disabled bool
	// OnDecision, when set, is called for every limited endpoint request
	// after the decision (logging, alerts, custom metrics). It must be fast.
	OnDecision func(context.Context, Event)

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
	// Prefix starts every bucket key. Default "rl". It is used as is, so it
	// may carry a Redis Cluster hash tag; braces in policy names and
	// identities are escaped and never form one.
	Prefix string
	// IPv6Prefix is the prefix length that groups IPv6 client addresses into
	// one IP bucket (ByIP, ByAnyIP and the IP part of ByCaller), between 32
	// and 128. Default 64. Use 56 or 48 when clients rotate addresses across
	// the /64s of their delegated prefix to get fresh IP buckets.
	IPv6Prefix int

	// Errors renders HTTP denials in Middleware (domain, localization). Nil
	// uses errs.WriteHTTP.
	Errors *errs.Handler

	// Logger receives the limiter logs. nil means log.Default().
	Logger        *log.Logger
	MeterProvider metric.MeterProvider
}

const defaultScope = "*"

// limit is a validated Limit with its bucket scope, header label and
// precomputed metric attributes (one option per result).
type limit struct {
	Limit
	scope  string // bucket scope: "<policy scope>" or "<policy scope>#<label>"
	bucket string // prefix + ":" + escaped scope + ":"
	label  string
	opts   map[string]metric.AddOption
}

var results = []string{"allowed", "limited", "dry_run", "degraded", "unavailable"}

// policy is a validated Policy.
type policy struct {
	Policy
	name    string // as shown to clients: the method/route or "default"
	limits  []limit
	visible int // limits the user declared (header ids get "#label" when > 1)
}

// Limiter enforces policies. It is safe for concurrent use and starts no
// goroutines.
type Limiter struct {
	store    Store
	remote   bool // store is not the in-memory one: use timeout + breaker
	fallback *LocalStore
	def      *policy
	policies map[string]*policy
	skip     map[string]bool

	subject, anonymous, device func(context.Context) (string, bool)

	exempt     map[string]bool
	exemptNets []netip.Prefix
	overrides  map[string]Rule

	dryRun     bool
	disabled   bool
	onDecision func(context.Context, Event)

	instances int
	timeout   time.Duration
	cooldown  time.Duration
	prefix    string
	v6bits    int
	log       *log.Logger
	errs      *errs.Handler
	now       func() time.Time

	openUntil atomic.Int64 // unix nanos; 0 = closed
	probing   atomic.Bool

	decisions   metric.Int64Counter
	storeErrors metric.Int64Counter
}

// New validates cfg and returns a Limiter.
func New(cfg Config) (*Limiter, error) {
	l := &Limiter{
		policies:   make(map[string]*policy, len(cfg.Policies)),
		skip:       make(map[string]bool, len(cfg.Skip)),
		subject:    cfg.Subject,
		anonymous:  cfg.Anonymous,
		device:     cfg.Device,
		exempt:     make(map[string]bool),
		overrides:  make(map[string]Rule, len(cfg.Overrides)),
		dryRun:     cfg.DryRun,
		disabled:   cfg.Disabled,
		onDecision: cfg.OnDecision,
		instances:  max(1, cfg.Instances),
		timeout:    cmpOr(cfg.StoreTimeout, 100*time.Millisecond),
		cooldown:   cmpOr(cfg.BreakerCooldown, time.Second),
		prefix:     cfg.Prefix,
		v6bits:     cmp.Or(cfg.IPv6Prefix, 64),
		log:        cfg.Logger,
		errs:       cfg.Errors,
		now:        time.Now,
	}
	if l.subject == nil {
		l.subject = userID
	}
	if l.device == nil {
		l.device = deviceID
	}
	if l.prefix == "" {
		l.prefix = "rl"
	}
	if l.log == nil {
		l.log = log.Default()
	}
	var problems []error
	if l.v6bits < 32 || l.v6bits > 128 {
		problems = append(problems, fmt.Errorf("IPv6Prefix must be in [32, 128], got %d", cfg.IPv6Prefix))
	}
	if cfg.Default != nil {
		p, err := resolve(*cfg.Default, defaultScope, "default")
		if err != nil {
			problems = append(problems, fmt.Errorf("default: %w", err))
		}
		l.def = p
	}
	for name, p := range cfg.Policies {
		if name == "" || name == defaultScope {
			problems = append(problems, fmt.Errorf("policy name %q is reserved", name))
			continue
		}
		rp, err := resolve(p, name, name)
		if err != nil {
			problems = append(problems, fmt.Errorf("policy %q: %w", name, err))
		}
		l.policies[name] = rp
	}
	for _, p := range append(slices.Collect(maps.Values(l.policies)), l.def) {
		if p == nil {
			continue
		}
		for i := range p.limits {
			p.limits[i].bucket = l.prefix + ":" + escapeKey(p.limits[i].scope, true) + ":"
		}
	}
	for _, s := range cfg.Skip {
		l.skip[s] = true
	}
	for _, e := range cfg.Exempt {
		ref, err := parseRef(e, true, l.v6bits)
		if err == nil && strings.HasPrefix(ref.key, "device:") {
			err = fmt.Errorf("%q: device ids are unverified and cannot be exempt", e)
		}
		if err != nil {
			problems = append(problems, fmt.Errorf("exempt %w", err))
			continue
		}
		if ref.prefix.IsValid() {
			l.exemptNets = append(l.exemptNets, ref.prefix)
		} else {
			l.exempt[ref.key] = true
		}
	}
	for k, r := range cfg.Overrides {
		ref, err := parseRef(k, false, l.v6bits)
		if err == nil {
			err = r.validate()
		}
		if err != nil {
			problems = append(problems, fmt.Errorf("override %q: %w", k, err))
			continue
		}
		l.overrides[ref.key] = r.normalize()
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
		metric.WithDescription("Rate-limit decisions by policy, deciding limit and result."),
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

// resolve validates p and names its limits. A policy with only Rule keeps
// the bucket scope of the policy; with several limits each gets
// "<scope>#<label>".
func resolve(p Policy, scope, name string) (*policy, error) {
	var all []Limit
	if p.Rule != (Rule{}) {
		all = append(all, Limit{Rule: p.Rule, By: ByCaller})
	}
	all = append(all, p.Limits...)
	if len(all) == 0 {
		return nil, errors.New("no limit: set Rule or Limits")
	}
	visible := len(all)
	// A ByCaller limit also counts the client IP when the caller only has an
	// unverified device id, so rotating fake ids does not get fresh buckets.
	for _, lim := range all[:visible] {
		if lim.By == ByCaller && lim.Key == nil {
			all = append(all, Limit{Rule: lim.Rule, By: byCallerDeviceIP})
		}
	}
	// Most specific first: a request denied by its own bucket stops before it
	// consumes the shared ones (IP, global).
	slices.SortStableFunc(all, func(a, b Limit) int { return cmp.Compare(a.rank(), b.rank()) })
	rp := &policy{Policy: p, name: name, visible: visible}
	seen := map[string]int{}
	var problems []error
	for i, lim := range all {
		if err := lim.validate(); err != nil {
			problems = append(problems, fmt.Errorf("limit %d: %w", i, err))
			continue
		}
		if !lim.By.valid() && lim.By != byCallerDeviceIP {
			problems = append(problems, fmt.Errorf("limit %d: unknown By %d", i, lim.By))
			continue
		}
		lim.Rule = lim.normalize()
		label := lim.By.String()
		if lim.Key != nil {
			label = "custom"
		}
		seen[label]++
		if n := seen[label]; n > 1 {
			label += "-" + strconv.Itoa(n)
		}
		s := scope
		if visible > 1 || lim.By == byCallerDeviceIP {
			s += "#" + label
		}
		opts := make(map[string]metric.AddOption, len(results))
		for _, r := range results {
			opts[r] = metric.WithAttributeSet(attribute.NewSet(attribute.String("policy", name),
				attribute.String("limit", label), attribute.String("result", r)))
		}
		rp.limits = append(rp.limits, limit{Limit: lim, scope: s, label: label, opts: opts})
	}
	return rp, errors.Join(problems...)
}

func cmpOr(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}

// policyFor returns the policy for name.
func (l *Limiter) policyFor(name string) (*policy, bool) {
	if l.disabled || l.skip[name] {
		return nil, false
	}
	if p, ok := l.policies[name]; ok {
		return p, true
	}
	return l.def, l.def != nil
}

// Limited reports whether requests for policy name are subject to any limit.
func (l *Limiter) Limited(name string) bool {
	_, ok := l.policyFor(name)
	return ok
}

// exempted reports whether id is listed in Config.Exempt.
func (l *Limiter) exempted(id identity) bool {
	if len(l.exempt) == 0 && len(l.exemptNets) == 0 {
		return false
	}
	for _, k := range id.keys() {
		if l.exempt[k] {
			return true
		}
	}
	if id.addr.IsValid() {
		for _, p := range l.exemptNets {
			if p.Contains(id.addr) {
				return true
			}
		}
	}
	return false
}

// part is the decision of one limit.
type part struct {
	lim  *limit
	rule Rule
	d    Decision
}

// Check checks one request for policy name against every limit that applies
// to the caller in ctx (identity from Config.Subject/Anonymous/Device and the
// client IP in xcontext.Info). Use it for limits inside business logic, for
// example Check(ctx, "send-otp") with Policies["send-otp"]. The Decision is
// always usable; a non-nil error reports a store failure that was handled
// according to OnStoreError.
func (l *Limiter) Check(ctx context.Context, name string) (Decision, error) {
	var remote string
	if pr, ok := peer.FromContext(ctx); ok && pr.Addr != nil {
		remote = pr.Addr.String()
	}
	d, _, err := l.checkIdentity(ctx, name, remote)
	return d, err
}

func (l *Limiter) checkIdentity(ctx context.Context, name, remote string) (Decision, []part, error) {
	p, ok := l.policyFor(name)
	if !ok {
		return Decision{Allowed: true}, nil, nil
	}
	id := l.identify(ctx, remote)
	if l.exempted(id) {
		return Decision{Allowed: true}, nil, nil
	}
	return l.decide(ctx, p, func(lim limit) (string, Rule, bool) {
		if lim.Key != nil {
			k, ok := lim.Key(ctx)
			return "key:" + boundKey(k), lim.Rule, ok && k != ""
		}
		k, ok := id.key(lim.By)
		if !ok {
			return "", Rule{}, false
		}
		if r, ok := l.overrides[k]; ok {
			return k, r, true
		}
		return k, lim.Rule, true
	})
}

// Allow checks one request for policy name with a caller-chosen bucket key
// (for example "tenant:42"), used for every limit of the policy except
// ByGlobal ones. Keys live in their own namespace ("key:"), so they cannot
// drain a user or IP bucket; keys over 256 bytes are hashed. Exempt and
// Overrides do not apply.
func (l *Limiter) Allow(ctx context.Context, name, key string) (Decision, error) {
	p, ok := l.policyFor(name)
	if !ok || key == "" {
		return Decision{Allowed: true}, nil
	}
	key = "key:" + boundKey(key) // never collides with user:/anon:/device:/ip: buckets
	d, _, err := l.decide(ctx, p, func(lim limit) (string, Rule, bool) {
		switch {
		case lim.By == byCallerDeviceIP:
			return "", Rule{}, false // identity based; Allow has no identity
		case lim.By == ByGlobal && lim.Key == nil:
			return "global", lim.Rule, true
		}
		return key, lim.Rule, true
	})
	return d, err
}

// decide checks every limit of p that applies (keyOf returns its bucket key
// and rule) and combines the results.
func (l *Limiter) decide(ctx context.Context, p *policy, keyOf func(limit) (string, Rule, bool)) (Decision, []part, error) {
	parts := make([]part, 0, len(p.limits))
	var errList []error
	for i := range p.limits {
		lim := &p.limits[i]
		key, rule, ok := keyOf(*lim)
		if !ok {
			continue
		}
		d, err := l.check(ctx, p.OnStoreError, rule, lim.bucket+escapeKey(key, false))
		if err != nil {
			errList = append(errList, err)
		}
		parts = append(parts, part{lim: lim, rule: rule, d: d})
		if !d.Allowed {
			break // the more shared buckets are not consumed by a denied request
		}
	}
	if len(parts) == 0 {
		return Decision{Allowed: true}, nil, nil
	}
	best := 0
	degraded, unavailable, denied := false, false, false
	for i, pt := range parts {
		degraded = degraded || pt.d.Degraded
		unavailable = unavailable || pt.d.Unavailable
		b := parts[best].d
		switch {
		case !pt.d.Allowed && (!denied || pt.d.RetryAfter > b.RetryAfter):
			best, denied = i, true
		case !denied && pt.d.Remaining < b.Remaining:
			best = i
		}
	}
	d := parts[best].d
	d.Degraded, d.Unavailable = degraded, unavailable
	if unavailable {
		d.Allowed = false
	}

	result := "allowed"
	switch {
	case d.Unavailable:
		result = "unavailable"
	case !d.Allowed:
		result = "limited"
	case d.Degraded:
		result = "degraded"
	}
	// Dry run never blocks, not even when the store is down and the policy
	// says Deny; the metric still reports what would have happened.
	if !d.Allowed && (p.DryRun || l.dryRun) {
		if result == "limited" {
			result = "dry_run"
		}
		d.Allowed, d.RetryAfter, d.Unavailable = true, 0, false
		l.log.LogAttrs(ctx, log.LevelDebug, "rate limit dry run would deny",
			log.String("policy", p.name), log.String("limit", parts[best].lim.label))
	}
	label := parts[best].lim.label
	l.decisions.Add(ctx, 1, parts[best].lim.opts[result])
	if l.onDecision != nil {
		l.onDecision(ctx, Event{Policy: p.name, Limit: label, Result: result, Decision: d})
	}
	return d, parts, errors.Join(errList...)
}

func (l *Limiter) check(ctx context.Context, mode StoreErrorMode, r Rule, bucket string) (Decision, error) {
	if !l.remote {
		return l.store.Allow(ctx, bucket, r)
	}
	open := l.openUntil.Load()
	if open != 0 {
		// Open, or half-open with another request already probing.
		if l.now().UnixNano() < open || !l.probing.CompareAndSwap(false, true) {
			return l.degrade(ctx, mode, r, bucket, errStoreOpen)
		}
		defer l.probing.Store(false)
	}
	cctx, cancel := context.WithTimeout(ctx, l.timeout)
	d, err := l.store.Allow(cctx, bucket, r)
	cancel()
	if err != nil {
		// A caller that went away says nothing about the store's health.
		if ctx.Err() == nil {
			// Log once per opening (and per failed probe), not per request.
			if prev := l.openUntil.Swap(l.now().Add(l.cooldown).UnixNano()); prev == 0 || open != 0 {
				l.log.LogAttrs(ctx, log.LevelWarn, "rate-limit store failing; OnStoreError applies until it recovers",
					log.Err(err), log.Duration("retry_in", l.cooldown))
			}
		}
		l.storeErrors.Add(ctx, 1)
		return l.degrade(ctx, mode, r, bucket, fmt.Errorf("ratelimit: store: %w", err))
	}
	if open != 0 && l.openUntil.Swap(0) != 0 { // probe succeeded
		l.log.LogAttrs(ctx, log.LevelInfo, "rate-limit store recovered")
	}
	return d, nil
}

var errStoreOpen = errors.New("ratelimit: store circuit open")

func (l *Limiter) degrade(ctx context.Context, mode StoreErrorMode, r Rule, bucket string, err error) (Decision, error) {
	switch mode {
	case Allow:
		return Decision{Allowed: true, Limit: r.Rate, Remaining: r.Burst, Degraded: true}, err
	case Deny:
		return Decision{Limit: r.Rate, RetryAfter: l.cooldown, Degraded: true, Unavailable: true}, err
	default:
		d, _ := l.fallback.Allow(ctx, "fb:"+bucket, r.divide(l.instances))
		d.Degraded = true
		return d, err
	}
}

// Remote is a shared GCRA backend such as ratelimit/redis.Store. It uses
// plain types so a backend module does not need to import this package.
// GCRA consumes one request from bucket key (rate per per, bursts up to
// burst); an error means the backend could not decide.
type Remote interface {
	GCRA(ctx context.Context, key string, rate, burst int, per time.Duration) (allowed bool, remaining int, retryAfter, resetAfter time.Duration, err error)
}

// RemoteStore adapts a Remote to Store:
//
//	ratelimit.Config{Store: ratelimit.RemoteStore(redisStore), ...}
func RemoteStore(r Remote) Store { return remoteStore{r} }

type remoteStore struct{ r Remote }

func (s remoteStore) Allow(ctx context.Context, key string, r Rule) (Decision, error) {
	r = r.normalize()
	allowed, remaining, retryAfter, resetAfter, err := s.r.GCRA(ctx, key, r.Rate, r.Burst, r.Per)
	if err != nil {
		return Decision{}, err
	}
	d := Decision{Allowed: allowed, Limit: r.Rate, Remaining: remaining, ResetAfter: resetAfter}
	if !allowed {
		d.RetryAfter = retryAfter
	}
	return d, nil
}
