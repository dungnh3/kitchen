package gcp

import (
	"context"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
)

// SourceOption configures a Source.
type SourceOption func(*Source)

// WithMaxOutstanding sets the client's flow control limits (defaults 1000
// messages, 1 GB). They are sent to Pub/Sub as the per-stream limit of
// leased messages, so up to messages × WithStreams messages can be leased at
// once; the client itself releases a message's quota when the Source's
// callback returns, not when it is acked. The bound on messages being
// processed is the Subscriber's WithMaxInFlight: keep messages ≥ it so the
// engine, not Pub/Sub, applies backpressure.
func WithMaxOutstanding(messages, bytes int) SourceOption {
	return func(s *Source) {
		s.sub.ReceiveSettings.MaxOutstandingMessages, s.sub.ReceiveSettings.MaxOutstandingBytes = messages, bytes
	}
}

// WithMaxExtension sets how long the client keeps extending a message's ack
// deadline (default 60m). It must exceed the Subscriber's handler timeout
// times its attempts plus backoff.
func WithMaxExtension(d time.Duration) SourceOption {
	return func(s *Source) { s.sub.ReceiveSettings.MaxExtension = d }
}

// WithMinAckExtension sets the minimum duration of one lease extension
// (MinDurationPerAckExtension, 10s–600s).
func WithMinAckExtension(d time.Duration) SourceOption {
	return func(s *Source) { s.sub.ReceiveSettings.MinDurationPerAckExtension = d }
}

// WithMaxAckExtension sets the maximum duration of one lease extension
// (MaxDurationPerAckExtension, 10s–600s). It bounds how long a message stays
// leased after its subscriber dies before Pub/Sub redelivers it. By default
// the client picks the extension from observed ack latencies (up to 10m).
func WithMaxAckExtension(d time.Duration) SourceOption {
	return func(s *Source) { s.sub.ReceiveSettings.MaxDurationPerAckExtension = d }
}

// WithStreams sets the number of StreamingPull streams (default 1).
func WithStreams(n int) SourceOption {
	return func(s *Source) { s.sub.ReceiveSettings.NumGoroutines = n }
}

// WithSubscriptionConfig states the subscription's settings instead of
// reading them at Receive start, which needs the pubsub.subscriptions.get
// permission: ordered is its enable_message_ordering, maxDeliveryAttempts its
// dead-letter policy's max_delivery_attempts (0 without a policy). Wrong
// values break ordering or dead-lettering; see [Source.Redelivery].
func WithSubscriptionConfig(ordered bool, maxDeliveryAttempts int) SourceOption {
	return func(s *Source) {
		s.stated = &subConfig{ordered: ordered, maxDeliveryAttempts: max(maxDeliveryAttempts, 0)}
	}
}

// WithSourceLogger sets the logger (nil means log.Default()).
func WithSourceLogger(l *log.Logger) SourceOption { return func(s *Source) { s.log = l } }

// OrderingKeyAttribute is the attribute holding a message's ordering key on
// subscriptions without message ordering, where Message.Key is empty. Pub/Sub
// rejects published attributes starting with "goog", so it never collides
// with an attribute set by a publisher.
const OrderingKeyAttribute = "googclient_ordering_key"

const (
	// ackResultTimeout bounds the wait for one acknowledgement result.
	ackResultTimeout = time.Minute
	// configTimeout bounds reading the subscription settings.
	configTimeout = 10 * time.Second
	// orderedFenceTTL bounds the key fence on ordered subscriptions: Pub/Sub
	// redelivers a nacked message promptly, and the fence only has to cover
	// messages of the key already in transit.
	orderedFenceTTL = 5 * time.Second
)

// Source receives from a Pub/Sub subscription. Received messages have Topic
// set to the subscription name or ID passed to NewSource.
//
// Subscription settings: at the start of Receive the Source reads the
// subscription (GetSubscription, which needs the pubsub.subscriptions.get
// permission) to learn whether message ordering is enabled and the
// dead-letter policy's max delivery attempts. Without message ordering,
// messages are delivered with an empty Key and the ordering key in the
// [OrderingKeyAttribute] attribute. If the settings cannot be read, a warning
// is logged and the Source assumes an ordered subscription without a
// dead-letter policy (Key is the ordering key). [WithSubscriptionConfig]
// states the settings and skips the read.
//
// Acknowledgements: Ack and Nack never wait for Pub/Sub. On subscriptions
// with exactly-once delivery the Source waits for the result in the
// background (up to one minute) and logs a failed acknowledgement, which
// means the message may be redelivered; Receive waits for those results
// before it returns.
//
// Trace context: messages carry the W3C trace context in the "traceparent"
// and "tracestate" attributes (see [pubsub.Telemetry]). Messages published by
// a client with the Google library's built-in tracing
// (ClientConfig.EnableOpenTelemetryTracing) carry it as
// "googclient_traceparent"; the Source maps those to the standard names when
// the standard ones are absent, so the trace continues. Keep the built-in
// tracing off on clients used with this package, or spans are duplicated.
type Source struct {
	client *gpubsub.Client
	sub    *gpubsub.Subscriber
	name   string
	log    *log.Logger
	stated *subConfig

	cfg  atomic.Pointer[subConfig] // nil until known
	acks sync.WaitGroup            // background acknowledgement results
}

// subConfig is what the Source needs to know about the subscription.
type subConfig struct {
	ordered             bool
	maxDeliveryAttempts int
}

var (
	_ pubsub.Source             = (*Source)(nil)
	_ pubsub.RedeliveryAdvisor  = (*Source)(nil)
	_ pubsub.ConsumerIdentifier = (*Source)(nil)
)

// NewSource returns a Source for subscription (ID or full name). Per-stream
// flow control is enabled by default.
func NewSource(client *gpubsub.Client, subscription string, opts ...SourceOption) (*Source, error) {
	if client == nil || subscription == "" {
		return nil, errors.New("gcp: nil client or empty subscription")
	}
	sub := client.Subscriber(subscription)
	sub.ReceiveSettings.EnablePerStreamFlowControl = true
	s := &Source{client: client, sub: sub, name: subscription}
	for _, o := range opts {
		o(s)
	}
	s.log = log.OrDefault(s.log)
	if s.stated != nil {
		s.cfg.Store(s.stated)
	}
	return s, nil
}

// Consumer implements [pubsub.ConsumerIdentifier]: the subscription.
func (s *Source) Consumer() pubsub.Consumer { return pubsub.Consumer{Subscription: s.name} }

// Redelivery implements [pubsub.RedeliveryAdvisor]. On an ordered
// subscription Pub/Sub may hand over later messages of a key before it
// redelivers a nacked one, so the key fence is useful (bounded to 5s); on an
// unordered subscription messages are delivered without a Key and no fence
// applies. MaxDeliveryAttempts is the dead-letter policy's limit (0 without a
// policy). Until the settings are known it reports an ordered subscription
// without a dead-letter policy.
func (s *Source) Redelivery() pubsub.Redelivery {
	c := s.cfg.Load()
	switch {
	case c == nil:
		return pubsub.Redelivery{Fence: true, FenceTTL: orderedFenceTTL}
	case c.ordered:
		return pubsub.Redelivery{Fence: true, FenceTTL: orderedFenceTTL, MaxDeliveryAttempts: c.maxDeliveryAttempts}
	default:
		return pubsub.Redelivery{MaxDeliveryAttempts: c.maxDeliveryAttempts}
	}
}

// readConfig reads the subscription settings unless they were stated. On
// failure it logs a warning and keeps what is known.
func (s *Source) readConfig(ctx context.Context) {
	if s.stated != nil {
		return
	}
	rctx, cancel := context.WithTimeout(ctx, configTimeout)
	defer cancel()
	ps, err := s.client.SubscriptionAdminClient.GetSubscription(rctx, &pubsubpb.GetSubscriptionRequest{Subscription: s.sub.String()})
	if err != nil {
		if ctx.Err() == nil { // not shutting down
			s.log.WarnContext(ctx, "gcp: cannot read the subscription settings; assuming message ordering and no dead-letter policy",
				log.String("messaging.destination.subscription.name", s.name), log.Err(err))
		}
		return
	}
	s.cfg.Store(&subConfig{
		ordered:             ps.GetEnableMessageOrdering(),
		maxDeliveryAttempts: int(ps.GetDeadLetterPolicy().GetMaxDeliveryAttempts()),
	})
}

// System implements [pubsub.Source].
func (s *Source) System() string { return System }

// Receive implements [pubsub.Source]. With message ordering enabled on the
// subscription, the client calls fn sequentially per ordering key. It
// returns after every acknowledgement result has been checked.
func (s *Source) Receive(ctx context.Context, fn func(context.Context, pubsub.Delivery)) error {
	defer s.acks.Wait()
	s.readConfig(ctx)
	cfg := s.cfg.Load()
	keyed := cfg == nil || cfg.ordered
	return s.sub.Receive(ctx, func(ctx context.Context, m *gpubsub.Message) {
		attempt := 0
		if m.DeliveryAttempt != nil {
			attempt = *m.DeliveryAttempt
		}
		key, attrs := m.OrderingKey, traceAttributes(m.Attributes)
		if !keyed && key != "" {
			attrs = maps.Clone(attrs) // never mutate the client's map
			if attrs == nil {
				attrs = make(map[string]string, 1)
			}
			attrs[OrderingKeyAttribute] = key
			key = ""
		}
		fn(ctx, &delivery{s: s, raw: m, msg: &pubsub.Message{
			ID: m.ID, Topic: s.name, Key: key, Data: m.Data,
			Attributes: attrs, PublishTime: m.PublishTime, DeliveryAttempt: attempt,
		}})
	})
}

const googclientPrefix = "googclient_"

// traceAttributes returns attrs with "googclient_"-prefixed W3C trace fields
// copied to their standard names when no standard trace context is present.
func traceAttributes(attrs map[string]string) map[string]string {
	if _, ok := attrs["traceparent"]; ok {
		return attrs
	}
	tp, ok := attrs[googclientPrefix+"traceparent"]
	if !ok {
		return attrs
	}
	out := maps.Clone(attrs)
	out["traceparent"] = tp
	if ts, ok := attrs[googclientPrefix+"tracestate"]; ok {
		out["tracestate"] = ts
	}
	return out
}

type delivery struct {
	s   *Source
	raw *gpubsub.Message
	msg *pubsub.Message
}

func (d *delivery) Message() *pubsub.Message { return d.msg }

func (d *delivery) Ack()  { d.settled("ack", d.raw.AckWithResult()) }
func (d *delivery) Nack() { d.settled("nack", d.raw.NackWithResult()) }

// settled checks an acknowledgement result without blocking the caller. The
// client resolves it at once unless the subscription has exactly-once
// delivery; otherwise it is awaited in the background, tracked by s.acks.
func (d *delivery) settled(op string, r *gpubsub.AckResult) {
	select {
	case <-r.Ready():
		d.check(op, r)
	default:
		d.s.acks.Go(func() { d.check(op, r) })
	}
}

func (d *delivery) check(op string, r *gpubsub.AckResult) {
	ctx, cancel := context.WithTimeout(context.Background(), ackResultTimeout)
	defer cancel()
	status, err := r.Get(ctx)
	if err == nil && status == gpubsub.AcknowledgeStatusSuccess {
		return
	}
	d.s.log.Warn("gcp: acknowledgement not confirmed; the message may be redelivered",
		log.String("operation", op),
		log.String("messaging.destination.subscription.name", d.s.name),
		log.String("messaging.message.id", d.msg.ID),
		log.String("status", ackStatus(status)),
		log.Err(err))
}

func ackStatus(s gpubsub.AcknowledgeStatus) string {
	switch s {
	case gpubsub.AcknowledgeStatusSuccess:
		return "success"
	case gpubsub.AcknowledgeStatusPermissionDenied:
		return "permission_denied"
	case gpubsub.AcknowledgeStatusFailedPrecondition:
		return "failed_precondition"
	case gpubsub.AcknowledgeStatusInvalidAckID:
		return "invalid_ack_id"
	default:
		return "other"
	}
}
