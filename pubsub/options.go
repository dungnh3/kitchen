package pubsub

import (
	"strconv"
	"time"

	"github.com/dungnh3/kitchen/log"
)

// Option configures a [Subscriber].
type Option func(*options)

type options struct {
	concurrency         int
	maxInFlight         int
	maxInFlightSet      bool
	maxRetries          int
	retryBase           time.Duration
	retryMax            time.Duration
	handlerTimeout      time.Duration
	shutdownTimeout     time.Duration
	maxBatchSize        int
	flushInterval       time.Duration
	maxDeliveryAttempts int
	keyFence            KeyFenceMode
	keyFenceTTL         time.Duration
	deadLetter          DeadLetterFunc
	logger              *log.Logger
	telemetry           []TelemetryOption
}

func defaultOptions() options {
	return options{
		concurrency:     10,
		maxRetries:      3,
		retryBase:       time.Second,
		retryMax:        30 * time.Second,
		handlerTimeout:  time.Minute,
		shutdownTimeout: 30 * time.Second,
		maxBatchSize:    100,
		flushInterval:   time.Second,
		keyFenceTTL:     time.Minute,
	}
}

// WithConcurrency sets how many handler calls run at once (default 10). In
// batch mode it is the number of shards, each processing one batch at a time.
func WithConcurrency(n int) Option { return func(o *options) { o.concurrency = n } }

// WithMaxInFlight bounds received-but-unsettled messages. When reached, the
// source is blocked until messages settle, so a replica never holds more
// broker leases than it can work on and other replicas get the rest.
//
// The default is derived: 2×WithConcurrency for [NewSubscriber] and
// WithConcurrency×batch size for [NewBatchSubscriber]. Raise it for very
// fast handlers behind a high-latency broker, or when a hot key would
// otherwise fill the window and hold back other keys. In batch mode it must
// be at least the batch size.
func WithMaxInFlight(n int) Option {
	return func(o *options) { o.maxInFlight, o.maxInFlightSet = n, true }
}

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

// KeyFenceMode selects whether the subscriber fences a key after one of its
// messages is finally nacked. See [WithKeyFence].
type KeyFenceMode int

const (
	// KeyFenceAuto (the default) fences when the source reports that it
	// helps ([Redelivery].Fence through [RedeliveryAdvisor]) and always for
	// a source that does not report it: on for Kafka, the memory broker and
	// Pub/Sub ordered subscriptions (short TTL), off for SQS FIFO, whose
	// message groups already hold back the rest of the key.
	KeyFenceAuto KeyFenceMode = iota
	// KeyFenceOn always fences, with the WithKeyFenceTTL bound.
	KeyFenceOn
	// KeyFenceOff never fences. Messages of the key already queued in the
	// subscriber are still nacked after a final nack, so per-key order holds
	// for whatever the broker itself guarantees.
	KeyFenceOff
)

func (m KeyFenceMode) String() string {
	switch m {
	case KeyFenceAuto:
		return "auto"
	case KeyFenceOn:
		return "on"
	case KeyFenceOff:
		return "off"
	default:
		return "KeyFenceMode(" + strconv.Itoa(int(m)) + ")"
	}
}

// WithKeyFence selects the key fence mode (default [KeyFenceAuto]). While a
// key is fenced, every other message of the key is nacked until the broker
// redelivers the finally nacked message (same ID) or the fence expires, so
// the key is processed in order even when later messages were already
// handed over. A nack that is the broker's last one (its DeliveryAttempt
// reached [Redelivery].MaxDeliveryAttempts) never fences: that message goes
// to the broker's dead-letter destination and will not come back.
func WithKeyFence(m KeyFenceMode) Option { return func(o *options) { o.keyFence = m } }

// WithKeyFenceTTL bounds how long a key stays fenced (default 1m; a source
// may report a shorter bound for its broker). It must be positive; turn
// fencing off with WithKeyFence(KeyFenceOff).
func WithKeyFenceTTL(d time.Duration) Option { return func(o *options) { o.keyFenceTTL = d } }

// WithDeadLetter sets the hook for permanently failed messages. Without it,
// such messages are logged at Error level and acked.
func WithDeadLetter(f DeadLetterFunc) Option { return func(o *options) { o.deadLetter = f } }

// WithLogger sets the logger (nil means log.Default()).
func WithLogger(l *log.Logger) Option { return func(o *options) { o.logger = l } }

// WithTelemetry configures tracing, metrics and propagation.
func WithTelemetry(opts ...TelemetryOption) Option {
	return func(o *options) { o.telemetry = append(o.telemetry, opts...) }
}
