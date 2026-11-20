// Package kafka implements [pubsub.Publisher] and [pubsub.Source] on Apache
// Kafka using github.com/twmb/franz-go.
//
// Message.Key is the record key, so messages with the same key go to the same
// partition and keep their order. Attributes are record headers. Message IDs
// are "topic/partition/offset".
//
// The Source joins a consumer group (cooperative-sticky balancing), commits
// only the contiguous prefix of settled offsets of each partition
// (at-least-once, never past an unprocessed record), and implements Nack as
// an in-process redelivery after a backoff that preserves per-key order.
// Kafka has no broker-side dead-letter queue: configure
// [pubsub.WithDeadLetter] (for example publishing to "<topic>.dlq") and
// [pubsub.WithMaxDeliveryAttempts], otherwise a poison message is redelivered
// forever and blocks its key.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dungnh3/kitchen/pubsub"
)

// System is the OpenTelemetry messaging.system value.
const System = "kafka"

// PublisherOption configures a Publisher.
type PublisherOption func(*Publisher)

// WithPublisherTelemetry configures tracing and metrics.
func WithPublisherTelemetry(opts ...pubsub.TelemetryOption) PublisherOption {
	return func(p *Publisher) { p.telOpts = append(p.telOpts, opts...) }
}

// Publisher produces records synchronously with a caller-owned client.
type Publisher struct {
	cl      *kgo.Client
	telOpts []pubsub.TelemetryOption
	tel     *pubsub.Telemetry
}

var _ pubsub.Publisher = (*Publisher)(nil)

// NewPublisher returns a Publisher. The client is owned by the caller; keep
// the franz-go defaults (idempotent producer) to preserve per-key order.
func NewPublisher(cl *kgo.Client, opts ...PublisherOption) (*Publisher, error) {
	if cl == nil {
		return nil, errors.New("kafka: nil client")
	}
	p := &Publisher{cl: cl}
	for _, o := range opts {
		o(p)
	}
	p.tel = pubsub.NewTelemetry(System, p.telOpts...)
	return p, nil
}

// Publish implements [pubsub.Publisher].
func (p *Publisher) Publish(ctx context.Context, topic string, msgs ...*pubsub.Message) ([]string, error) {
	return p.tel.Publish(ctx, topic, msgs, func(ctx context.Context, msgs []*pubsub.Message) ([]string, error) {
		if topic == "" {
			return nil, errors.New("kafka: empty topic")
		}
		recs := make([]*kgo.Record, len(msgs))
		for i, m := range msgs {
			r := &kgo.Record{Topic: topic, Value: m.Data}
			if m.Key != "" {
				r.Key = []byte(m.Key)
			}
			for _, k := range slices.Sorted(maps.Keys(m.Attributes)) {
				r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(m.Attributes[k])})
			}
			recs[i] = r
		}
		res := p.cl.ProduceSync(ctx, recs...)
		ids := make([]string, len(msgs))
		failed := map[int]error{}
		for i, r := range res {
			if r.Err != nil {
				if errors.Is(r.Err, kgo.ErrClientClosed) {
					r.Err = fmt.Errorf("%w: %w", pubsub.ErrClosed, r.Err)
				}
				failed[i] = r.Err
				continue
			}
			ids[i] = recordID(r.Record)
		}
		switch {
		case len(failed) == 0:
			return ids, nil
		case len(msgs) == 1:
			return ids, failed[0]
		default:
			return ids, &pubsub.PublishError{IDs: ids, Failed: failed}
		}
	})
}

// Close flushes buffered records. It does not close the client.
func (p *Publisher) Close(ctx context.Context) error { return p.cl.Flush(ctx) }

func recordID(r *kgo.Record) string {
	return fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset)
}
