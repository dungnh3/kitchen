package gcp

import (
	"context"
	"errors"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"

	"github.com/dungnh3/kitchen/pubsub"
)

// SourceOption configures a Source.
type SourceOption func(*gpubsub.ReceiveSettings)

// WithMaxOutstanding bounds unacked messages and bytes held by the client
// (defaults 1000 messages, 1 GB). Keep messages ≥ the Subscriber's
// WithMaxInFlight so the engine, not the client, applies backpressure.
func WithMaxOutstanding(messages, bytes int) SourceOption {
	return func(s *gpubsub.ReceiveSettings) {
		s.MaxOutstandingMessages, s.MaxOutstandingBytes = messages, bytes
	}
}

// WithMaxExtension sets how long the client keeps extending a message's ack
// deadline (default 60m). It must exceed the Subscriber's handler timeout
// times its attempts plus backoff.
func WithMaxExtension(d time.Duration) SourceOption {
	return func(s *gpubsub.ReceiveSettings) { s.MaxExtension = d }
}

// WithMinAckExtension sets the minimum duration of one lease extension
// (MinDurationPerAckExtension, 10s–600s).
func WithMinAckExtension(d time.Duration) SourceOption {
	return func(s *gpubsub.ReceiveSettings) { s.MinDurationPerAckExtension = d }
}

// WithStreams sets the number of StreamingPull streams (default 1).
func WithStreams(n int) SourceOption {
	return func(s *gpubsub.ReceiveSettings) { s.NumGoroutines = n }
}

// Source receives from a Pub/Sub subscription. Received messages have Topic
// set to the subscription name or ID passed to NewSource.
type Source struct {
	sub  *gpubsub.Subscriber
	name string
}

var _ pubsub.Source = (*Source)(nil)

// NewSource returns a Source for subscription (ID or full name). Per-stream
// flow control is enabled by default.
func NewSource(client *gpubsub.Client, subscription string, opts ...SourceOption) (*Source, error) {
	if client == nil || subscription == "" {
		return nil, errors.New("gcp: nil client or empty subscription")
	}
	sub := client.Subscriber(subscription)
	sub.ReceiveSettings.EnablePerStreamFlowControl = true
	for _, o := range opts {
		o(&sub.ReceiveSettings)
	}
	return &Source{sub: sub, name: subscription}, nil
}

// System implements [pubsub.Source].
func (s *Source) System() string { return System }

// Receive implements [pubsub.Source]. With message ordering enabled on the
// subscription, the client calls fn sequentially per ordering key.
func (s *Source) Receive(ctx context.Context, fn func(context.Context, pubsub.Delivery)) error {
	return s.sub.Receive(ctx, func(ctx context.Context, m *gpubsub.Message) {
		attempt := 0
		if m.DeliveryAttempt != nil {
			attempt = *m.DeliveryAttempt
		}
		fn(ctx, &delivery{raw: m, msg: &pubsub.Message{
			ID: m.ID, Topic: s.name, Key: m.OrderingKey, Data: m.Data,
			Attributes: m.Attributes, PublishTime: m.PublishTime, DeliveryAttempt: attempt,
		}})
	})
}

type delivery struct {
	raw *gpubsub.Message
	msg *pubsub.Message
}

func (d *delivery) Message() *pubsub.Message { return d.msg }
func (d *delivery) Ack()                     { d.raw.Ack() }
func (d *delivery) Nack()                    { d.raw.Nack() }
