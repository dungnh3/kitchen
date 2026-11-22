package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/otelx"
)

const scopeName = "github.com/dungnh3/kitchen/pubsub"

// TelemetryOption configures tracing, metrics and context propagation.
type TelemetryOption func(*telemetryConfig)

type telemetryConfig struct {
	tp   trace.TracerProvider
	mp   metric.MeterProvider
	prop propagation.TextMapPropagator
}

// WithTracerProvider sets the TracerProvider (default: otel.GetTracerProvider).
// Pass a noop provider to disable tracing.
func WithTracerProvider(tp trace.TracerProvider) TelemetryOption {
	return func(c *telemetryConfig) { c.tp = tp }
}

// WithMeterProvider sets the MeterProvider (default: otel.GetMeterProvider).
func WithMeterProvider(mp metric.MeterProvider) TelemetryOption {
	return func(c *telemetryConfig) { c.mp = mp }
}

// WithPropagator sets the propagator used to inject and extract the trace
// context in message attributes (default: otel.GetTextMapPropagator).
func WithPropagator(p propagation.TextMapPropagator) TelemetryOption {
	return func(c *telemetryConfig) { c.prop = p }
}

// Telemetry instruments publish and process operations for one messaging
// system. Backends create one per Publisher/Subscriber; it is exported so that
// third-party backends and the outbox can reuse it.
type Telemetry struct {
	system   string
	tracer   trace.Tracer
	prop     propagation.TextMapPropagator
	opDur    metric.Float64Histogram
	sent     metric.Int64Counter
	consumed metric.Int64Counter
	procDur  metric.Float64Histogram
	settledC metric.Int64Counter
	retries  metric.Int64Counter
	inflight metric.Int64UpDownCounter

	// opts caches the measurement attributes of the consumer metrics per
	// topic, outcome and operation: building them allocates, and the set is
	// small (topics × outcomes).
	opts sync.Map // string -> metric.MeasurementOption

	// consumer holds the consumer identity attributes (see
	// ConsumerIdentifier), set once by the subscriber before use.
	consumer []attribute.KeyValue
}

// setConsumer records the consumer identity added to consumer telemetry.
func (t *Telemetry) setConsumer(c Consumer) {
	if c.Group != "" {
		t.consumer = append(t.consumer, semconv.MessagingConsumerGroupName(c.Group))
	}
	if c.Subscription != "" {
		t.consumer = append(t.consumer, semconv.MessagingDestinationSubscriptionName(c.Subscription))
	}
}

// withConsumer appends the consumer identity to attrs.
func (t *Telemetry) withConsumer(attrs ...attribute.KeyValue) []attribute.KeyValue {
	return append(attrs, t.consumer...)
}

// Settlement outcomes recorded by the subscriber on
// kitchen.pubsub.settled.messages (attribute kitchen.pubsub.outcome).
const (
	outcomeAck        = "ack"
	outcomeNack       = "nack"        // handler failed; the broker redelivers
	outcomeDeadLetter = "dead_letter" // permanent failure, DeadLetter hook ran
	outcomeOrdering   = "ordering"    // nacked unprocessed to keep its key in order
	outcomeShutdown   = "shutdown"    // nacked unprocessed because Run is stopping
)

// durationBuckets are the bucket boundaries the OpenTelemetry messaging
// semantic conventions advise for duration histograms, in seconds.
var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10}

// NewTelemetry returns instrumentation for system (an OTel messaging.system
// value such as "gcp_pubsub").
func NewTelemetry(system string, opts ...TelemetryOption) *Telemetry {
	c := telemetryConfig{}
	for _, o := range opts {
		o(&c)
	}
	if c.tp == nil {
		c.tp = otel.GetTracerProvider()
	}
	if c.mp == nil {
		c.mp = otel.GetMeterProvider()
	}
	if c.prop == nil {
		c.prop = otel.GetTextMapPropagator()
	}
	meter := c.mp.Meter(scopeName)
	t := &Telemetry{
		system: system,
		tracer: c.tp.Tracer(scopeName),
		prop:   c.prop,
	}
	// Instrument creation only fails for invalid names, which are constants
	// here; a failed instrument is a no-op, so the errors are ignored.
	t.opDur, _ = meter.Float64Histogram("messaging.client.operation.duration",
		metric.WithUnit("s"), metric.WithDescription("Duration of messaging operation initiated by a producer or consumer client."),
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	t.sent, _ = meter.Int64Counter("messaging.client.sent.messages",
		metric.WithUnit("{message}"), metric.WithDescription("Number of messages producer attempted to send to the broker."))
	t.consumed, _ = meter.Int64Counter("messaging.client.consumed.messages",
		metric.WithUnit("{message}"), metric.WithDescription("Number of messages that were delivered to the application."))
	t.procDur, _ = meter.Float64Histogram("messaging.process.duration",
		metric.WithUnit("s"), metric.WithDescription("Duration of processing operation."),
		metric.WithExplicitBucketBoundaries(durationBuckets...))
	t.settledC, _ = meter.Int64Counter("kitchen.pubsub.settled.messages",
		metric.WithUnit("{message}"), metric.WithDescription("Messages settled by the subscriber, by outcome."))
	t.retries, _ = meter.Int64Counter("kitchen.pubsub.retries",
		metric.WithUnit("{retry}"), metric.WithDescription("In-process handler retries."))
	t.inflight, _ = meter.Int64UpDownCounter("kitchen.pubsub.inflight.messages",
		metric.WithUnit("{message}"), metric.WithDescription("Messages received by the subscriber and not yet settled."))
	return t
}

// Inject writes the trace context of ctx into attrs.
func (t *Telemetry) Inject(ctx context.Context, attrs map[string]string) {
	t.prop.Inject(ctx, propagation.MapCarrier(attrs))
}

// injectMissing injects the trace context of ctx unless attrs already carry
// one (e.g. a message relayed from an outbox keeps its original trace).
func (t *Telemetry) injectMissing(ctx context.Context, attrs map[string]string) {
	for _, f := range t.prop.Fields() {
		if _, ok := attrs[f]; ok {
			return
		}
	}
	t.Inject(ctx, attrs)
}

// Extract returns ctx carrying the trace context found in attrs.
func (t *Telemetry) Extract(ctx context.Context, attrs map[string]string) context.Context {
	if len(attrs) == 0 {
		return ctx
	}
	return t.prop.Extract(ctx, propagation.MapCarrier(attrs))
}

func (t *Telemetry) baseAttrs(op, opType, topic string) []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.MessagingSystemKey.String(t.system),
		semconv.MessagingOperationNameKey.String(op),
		semconv.MessagingOperationTypeKey.String(opType),
		semconv.MessagingDestinationName(topic),
	}
}

// Publish instruments send: it starts a PRODUCER span, injects the trace
// context into copies of msgs (keeping a trace context already present in a
// message's attributes) and calls send with the copies.
func (t *Telemetry) Publish(ctx context.Context, topic string, msgs []*Message,
	send func(ctx context.Context, msgs []*Message) ([]string, error),
) ([]string, error) {
	attrs := t.baseAttrs("send", "send", topic)
	if len(msgs) > 1 {
		attrs = append(attrs, semconv.MessagingBatchMessageCount(len(msgs)))
	}
	ctx, span := t.tracer.Start(ctx, "send "+topic,
		trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs...))
	defer span.End()

	out := make([]*Message, len(msgs))
	for i, m := range msgs {
		if m == nil {
			err := errs.Wrap(fmt.Errorf("pubsub: message %d is nil", i), errs.ErrInvalidArgument)
			otelx.RecordError(span, err)
			return nil, err
		}
		c := m.Clone()
		if c.Attributes == nil {
			c.Attributes = make(map[string]string, 2)
		}
		t.injectMissing(ctx, c.Attributes)
		out[i] = c
	}

	start := time.Now()
	ids, err := send(ctx, out)
	err = asDependencyError(err)
	mattrs := attrs[:4:4]
	if err != nil {
		et := errorType(err)
		otelx.RecordError(span, err)
		mattrs = append(mattrs, semconv.ErrorTypeKey.String(et))
	}
	t.opDur.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(mattrs...))
	t.sent.Add(ctx, int64(len(msgs)), metric.WithAttributes(mattrs...))
	return ids, err
}

// consumedOne records one message delivered to the engine.
func (t *Telemetry) consumedOne(ctx context.Context, topic string) {
	t.consumed.Add(ctx, 1, t.cachedOpt("r\x00"+topic, func() []attribute.KeyValue {
		return t.withConsumer(
			semconv.MessagingSystemKey.String(t.system),
			semconv.MessagingOperationNameKey.String("receive"),
			semconv.MessagingDestinationName(topic))
	}))
}

// cachedOpt returns the cached measurement option for key, building it
// with build on first use.
func (t *Telemetry) cachedOpt(key string, build func() []attribute.KeyValue) metric.MeasurementOption {
	if o, ok := t.opts.Load(key); ok {
		return o.(metric.MeasurementOption) //nolint:forcetypeassert // only this function stores
	}
	o, _ := t.opts.LoadOrStore(key, metric.WithAttributes(build()...))
	return o.(metric.MeasurementOption) //nolint:forcetypeassert // only this function stores
}

func (t *Telemetry) consumerAttrs(topic string) metric.MeasurementOption {
	return t.cachedOpt("c\x00"+topic, func() []attribute.KeyValue {
		return t.withConsumer(semconv.MessagingSystemKey.String(t.system), semconv.MessagingDestinationName(topic))
	})
}

// settled records how the subscriber settled one message.
func (t *Telemetry) settled(ctx context.Context, topic, outcome string) {
	t.settledC.Add(ctx, 1, t.cachedOpt("s\x00"+outcome+"\x00"+topic, func() []attribute.KeyValue {
		return t.withConsumer(
			semconv.MessagingSystemKey.String(t.system), semconv.MessagingDestinationName(topic),
			attribute.String("kitchen.pubsub.outcome", outcome))
	}))
}

// retried records one in-process retry.
func (t *Telemetry) retried(ctx context.Context, topic string) {
	t.retries.Add(ctx, 1, t.consumerAttrs(topic))
}

// inflightAdd tracks received-but-unsettled messages.
func (t *Telemetry) inflightAdd(ctx context.Context, n int64, topic string) {
	t.inflight.Add(ctx, n, t.consumerAttrs(topic))
}

// startProcess starts a CONSUMER span for msgs. For a single message the
// producer context becomes the parent; links to every producer context are
// always added.
func (t *Telemetry) startProcess(ctx context.Context, msgs []*Message) (context.Context, trace.Span) {
	topic := ""
	if len(msgs) > 0 {
		topic = msgs[0].Topic
	}
	attrs := t.withConsumer(t.baseAttrs("process", "process", topic)...)
	links := make([]trace.Link, 0, len(msgs))
	for _, m := range msgs {
		if sc := trace.SpanContextFromContext(t.Extract(context.Background(), m.Attributes)); sc.IsValid() {
			links = append(links, trace.Link{SpanContext: sc})
		}
	}
	if len(msgs) == 1 {
		m := msgs[0]
		ctx = t.Extract(ctx, m.Attributes)
		if m.ID != "" {
			attrs = append(attrs, semconv.MessagingMessageID(m.ID))
		}
		if m.DeliveryAttempt > 0 {
			attrs = append(attrs, attribute.Int("messaging.message.delivery_attempt", m.DeliveryAttempt))
		}
	} else {
		attrs = append(attrs, semconv.MessagingBatchMessageCount(len(msgs)))
	}
	return t.tracer.Start(ctx, "process "+topic,
		trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(attrs...), trace.WithLinks(links...))
}

func (t *Telemetry) endProcess(ctx context.Context, span trace.Span, topic string, start time.Time, err error) {
	et := ""
	if err != nil {
		otelx.RecordError(span, err)
		et = errorType(err)
	}
	span.End()
	t.procDur.Record(ctx, time.Since(start).Seconds(), t.cachedOpt("p\x00"+et+"\x00"+topic, func() []attribute.KeyValue {
		attrs := t.withConsumer(
			semconv.MessagingSystemKey.String(t.system),
			semconv.MessagingOperationNameKey.String("process"),
			semconv.MessagingDestinationName(topic))
		if et != "" {
			attrs = append(attrs, semconv.ErrorTypeKey.String(et))
		}
		return attrs
	}))
}

func errorType(err error) string {
	var pe *PublishError
	if errors.As(err, &pe) {
		return "partial"
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, ErrPermanent):
		return "permanent"
	case errors.Is(err, ErrKeyPaused):
		return "key_paused"
	default:
		return semconv.ErrorTypeOther.Value.AsString()
	}
}

// asDependencyError marks a broker failure as errs.ErrDependencyUnavailable
// (503) unless err already carries an errs code or is a context error, so a
// failed Publish surfaces to callers as a dependency outage, not a 500. A
// broker status that rejects the request itself (for example
// INVALID_ARGUMENT for an oversized message) keeps its code: retrying it
// cannot help, so it must not look like an outage.
func asDependencyError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if _, ok := errors.AsType[*errs.Error](err); ok {
		return err
	}
	if s := rejectedBy(errs.Code(err)); s != nil {
		return errs.Wrap(err, s)
	}
	return errs.Wrap(err, errs.ErrDependencyUnavailable)
}

// rejectedBy returns the errs sentinel for a broker status code that rejects
// the request itself, or nil for codes that mean the broker is unavailable.
func rejectedBy(c codes.Code) *errs.Error {
	switch c {
	case codes.InvalidArgument:
		return errs.ErrInvalidArgument
	case codes.NotFound:
		return errs.ErrNotFound
	case codes.PermissionDenied:
		return errs.ErrPermissionDenied
	case codes.Unauthenticated:
		return errs.ErrUnauthenticated
	case codes.FailedPrecondition:
		return errs.ErrFailedPrecondition
	case codes.OutOfRange:
		return errs.ErrOutOfRange
	default:
		return nil
	}
}
