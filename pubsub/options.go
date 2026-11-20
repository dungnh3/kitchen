package pubsub

import (
	"log/slog"
	"time"
)

// Option configures a [Subscriber].
type Option func(*options)

type options struct {
	concurrency         int
	maxInFlight         int
	maxRetries          int
	retryBase           time.Duration
	retryMax            time.Duration
	handlerTimeout      time.Duration
	shutdownTimeout     time.Duration
	maxBatchSize        int
	flushInterval       time.Duration
	maxDeliveryAttempts int
	deadLetter          DeadLetterFunc
	logger              *slog.Logger
	telemetry           []TelemetryOption
}

func defaultOptions() options {
	return options{
		concurrency:     10,
		maxInFlight:     1000,
		maxRetries:      3,
		retryBase:       time.Second,
		retryMax:        30 * time.Second,
		handlerTimeout:  time.Minute,
		shutdownTimeout: 30 * time.Second,
		maxBatchSize:    100,
		flushInterval:   time.Second,
	}
}

// WithConcurrency sets how many handler calls run at once (default 10). In
// batch mode it is the number of shards, each processing one batch at a time.
func WithConcurrency(n int) Option { return func(o *options) { o.concurrency = n } }

// WithMaxInFlight bounds received-but-unsettled messages (default 1000). When
// reached, the source is blocked until messages settle.
func WithMaxInFlight(n int) Option { return func(o *options) { o.maxInFlight = n } }

// WithRetry sets the number of in-process retries after the first failed
// attempt (default 3) and the exponential backoff bounds (default 1s doubling
// up to 30s, with jitter). maxRetries 0 disables in-process retries.
func WithRetry(maxRetries int, base, maxDelay time.Duration) Option {
	return func(o *options) {
		o.maxRetries, o.retryBase, o.retryMax = maxRetries, base, maxDelay
	}
}

// WithHandlerTimeout bounds each handler attempt (default 1m). Keep it below
// the broker's maximum lease extension. 0 disables the timeout.
func WithHandlerTimeout(d time.Duration) Option { return func(o *options) { o.handlerTimeout = d } }

// WithShutdownTimeout bounds how long Run waits for running handlers after its
// context is cancelled before cancelling their contexts (default 30s).
func WithShutdownTimeout(d time.Duration) Option { return func(o *options) { o.shutdownTimeout = d } }

// WithBatch sets the maximum batch size (default 100) and how long a partial
// batch may wait (default 1s). Only used by NewBatchSubscriber.
func WithBatch(maxSize int, flushInterval time.Duration) Option {
	return func(o *options) { o.maxBatchSize, o.flushInterval = maxSize, flushInterval }
}

// WithMaxDeliveryAttempts treats a message whose broker DeliveryAttempt is at
// least n as permanently failed after its last in-process attempt fails, so it
// goes to the DeadLetter hook instead of being nacked again. 0 (default)
// disables the check and leaves dead-lettering to the broker's policy.
func WithMaxDeliveryAttempts(n int) Option { return func(o *options) { o.maxDeliveryAttempts = n } }

// WithDeadLetter sets the hook for permanently failed messages. Without it,
// such messages are logged at Error level and acked.
func WithDeadLetter(f DeadLetterFunc) Option { return func(o *options) { o.deadLetter = f } }

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

// WithTelemetry configures tracing, metrics and propagation.
func WithTelemetry(opts ...TelemetryOption) Option {
	return func(o *options) { o.telemetry = append(o.telemetry, opts...) }
}
