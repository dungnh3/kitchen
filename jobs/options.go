package jobs

import (
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/dungnh3/kitchen/log"
)

// Queue is a named queue and how many of its jobs run at once in this
// process.
type Queue struct {
	Name    string
	Workers int
}

// Periodic is a job inserted at every multiple of Interval (counted from the
// Unix epoch), once across all replicas. Each run is a normal job, with
// retries. A slot that passes while every replica is down is skipped.
type Periodic struct {
	Interval time.Duration
	// New returns the args of a run.
	New func() Args
	// Opts are the insert options of a run; nil means the defaults.
	Opts *InsertOpts
}

// Every returns a [Periodic] that runs every interval (at least 1 second).
func Every(interval time.Duration, newArgs func() Args) Periodic {
	return Periodic{Interval: interval, New: newArgs}
}

// Option configures a Client.
type Option func(*options)

type options struct {
	registry    *Registry
	queues      []Queue
	poll        time.Duration
	lease       time.Duration
	backoffBase time.Duration
	backoffMax  time.Duration
	maxAttempts int
	shutdown    time.Duration
	jobTimeout  time.Duration
	retainDone  time.Duration
	retainDead  time.Duration
	cleanupEach time.Duration
	periodic    []Periodic
	logger      *log.Logger
	tp          trace.TracerProvider
	mp          metric.MeterProvider
	prop        propagation.TextMapPropagator
}

func defaultOptions() options {
	return options{
		queues:      []Queue{{Name: "default", Workers: 10}},
		poll:        time.Second,
		lease:       30 * time.Second,
		backoffBase: time.Second,
		backoffMax:  time.Hour,
		maxAttempts: 20,
		shutdown:    30 * time.Second,
		jobTimeout:  time.Minute,
		retainDone:  24 * time.Hour,
		retainDead:  7 * 24 * time.Hour,
		cleanupEach: time.Minute,
	}
}

func (o *options) validate() error {
	var errs []error
	if len(o.queues) == 0 {
		errs = append(errs, errors.New("no queues"))
	}
	seen := map[string]bool{}
	for _, q := range o.queues {
		switch {
		case !queueRE.MatchString(q.Name):
			errs = append(errs, fmt.Errorf("invalid queue name %q", q.Name))
		case q.Workers < 1:
			errs = append(errs, fmt.Errorf("queue %q: Workers must be at least 1", q.Name))
		case seen[q.Name]:
			errs = append(errs, fmt.Errorf("duplicate queue %q", q.Name))
		}
		seen[q.Name] = true
	}
	if o.poll <= 0 || o.lease < time.Second || o.backoffBase <= 0 || o.backoffMax < o.backoffBase ||
		o.maxAttempts < 1 || o.shutdown < 0 || o.retainDone < 0 || o.retainDead < 0 || o.cleanupEach <= 0 {
		errs = append(errs, errors.New("invalid duration or attempt option"))
	}
	for i, p := range o.periodic {
		if p.Interval < time.Second || p.New == nil {
			errs = append(errs, fmt.Errorf("periodic job %d: needs an Interval of at least 1s and a New func", i))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("jobs: %w", errors.Join(errs...))
	}
	return nil
}

// WithRegistry sets the workers. A client without a registry can insert
// jobs but not run them.
func WithRegistry(r *Registry) Option { return func(o *options) { o.registry = r } }

// WithQueues sets the queues this client works (default "default" with 10
// workers). Inserting into a queue is not limited to these.
func WithQueues(q ...Queue) Option { return func(o *options) { o.queues = q } }

// WithPeriodic adds periodic jobs.
func WithPeriodic(p ...Periodic) Option {
	return func(o *options) { o.periodic = append(o.periodic, p...) }
}

// WithPollInterval sets how often an idle queue polls (default 1s).
func WithPollInterval(d time.Duration) Option { return func(o *options) { o.poll = d } }

// WithLease sets how long a claim lasts without renewal (default 30s, at
// least 1s). It is renewed every lease/3 while the job runs.
func WithLease(d time.Duration) Option { return func(o *options) { o.lease = d } }

// WithRetryBackoff sets the delay before attempt n+1: base·2^(n-1) capped at
// maxDelay, with jitter (default 1s, 1h).
func WithRetryBackoff(base, maxDelay time.Duration) Option {
	return func(o *options) { o.backoffBase, o.backoffMax = base, maxDelay }
}

// WithMaxAttempts sets the default number of attempts (default 20, about
// 20 hours with the default backoff).
func WithMaxAttempts(n int) Option { return func(o *options) { o.maxAttempts = n } }

// WithJobTimeout sets the default bound of one attempt (default 1 minute; 0
// or negative means no timeout). A worker's Timeout method overrides it.
func WithJobTimeout(d time.Duration) Option { return func(o *options) { o.jobTimeout = d } }

// WithShutdownTimeout sets how long Run waits for running jobs after its
// context is done before it cancels them (default 30s). A job cancelled this
// way is given back without spending its attempt.
func WithShutdownTimeout(d time.Duration) Option { return func(o *options) { o.shutdown = d } }

// WithRetention sets how long completed jobs, and cancelled and discarded
// jobs, are kept before the clean-up deletes them (default 24h and 7 days).
func WithRetention(completed, discarded time.Duration) Option {
	return func(o *options) { o.retainDone, o.retainDead = completed, discarded }
}

// WithCleanupInterval sets how often each replica deletes expired rows
// (default 1 minute).
func WithCleanupInterval(d time.Duration) Option { return func(o *options) { o.cleanupEach = d } }

// WithLogger sets the logger (nil means log.Default()).
func WithLogger(l *log.Logger) Option { return func(o *options) { o.logger = l } }

// WithTracerProvider sets the TracerProvider (default: the OTel global).
func WithTracerProvider(tp trace.TracerProvider) Option { return func(o *options) { o.tp = tp } }

// WithMeterProvider sets the MeterProvider (default: the OTel global).
func WithMeterProvider(mp metric.MeterProvider) Option { return func(o *options) { o.mp = mp } }

// WithPropagator sets the propagator that carries the trace context from the
// enqueuer to the worker (default: the OTel global).
func WithPropagator(p propagation.TextMapPropagator) Option { return func(o *options) { o.prop = p } }
