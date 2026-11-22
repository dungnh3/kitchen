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
//
// Ordering on publish: when a keyed message fails to publish, its key is
// paused on that topic: later messages of the key fail with an error wrapping
// [pubsub.ErrKeyPaused] until [Publisher.ResumeKey], so a retry cannot land
// after newer messages. Messages of the key sent in the same Publish call as
// the failed one are produced concurrently and may already be written.
//
// Shutdown: set [WithRevokeTimeout] at or above the Subscriber's
// [pubsub.WithShutdownTimeout], or acks of handlers still draining at
// shutdown are not committed and those records are redelivered.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dungnh3/kitchen/errs"
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

	mu     sync.Mutex
	closed bool
	paused map[string]map[string]struct{} // topic -> paused keys
}

var (
	_ pubsub.Publisher  = (*Publisher)(nil)
	_ pubsub.KeyResumer = (*Publisher)(nil)
)

// NewPublisher returns a Publisher. The client is owned by the caller; keep
// the franz-go defaults (idempotent producer) to preserve per-key order.
func NewPublisher(cl *kgo.Client, opts ...PublisherOption) (*Publisher, error) {
	if cl == nil {
		return nil, errors.New("kafka: nil client")
	}
	p := &Publisher{cl: cl, paused: map[string]map[string]struct{}{}}
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
			return nil, errs.Wrap(errors.New("kafka: empty topic"), errs.ErrInvalidArgument)
		}
		if p.isClosed() {
			return nil, fmt.Errorf("kafka: publish to %s: %w", topic, pubsub.ErrClosed)
		}
		ids := make([]string, len(msgs))
		failed := map[int]error{}
		recs := make([]*kgo.Record, 0, len(msgs))
		idx := make([]int, 0, len(msgs)) // recs[j] is msgs[idx[j]]
		for i, m := range msgs {
			if p.isPaused(topic, m.Key) {
				failed[i] = fmt.Errorf("kafka: topic %s key %q: %w", topic, m.Key, pubsub.ErrKeyPaused)
				continue
			}
			r := &kgo.Record{Topic: topic, Value: m.Data}
			if m.Key != "" {
				r.Key = []byte(m.Key)
			}
			for _, k := range slices.Sorted(maps.Keys(m.Attributes)) {
				r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(m.Attributes[k])})
			}
			recs = append(recs, r)
			idx = append(idx, i)
		}
		var res kgo.ProduceResults
		if len(recs) > 0 {
			res = p.cl.ProduceSync(ctx, recs...)
		}
		for j, r := range res {
			i := idx[j]
			if r.Err != nil {
				if errors.Is(r.Err, kgo.ErrClientClosed) {
					r.Err = fmt.Errorf("%w: %w", pubsub.ErrClosed, r.Err)
				}
				failed[i] = r.Err
				p.pause(topic, msgs[i].Key)
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

func (p *Publisher) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func (p *Publisher) pause(topic, key string) {
	if key == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused[topic] == nil {
		p.paused[topic] = map[string]struct{}{}
	}
	p.paused[topic][key] = struct{}{}
}

func (p *Publisher) isPaused(topic, key string) bool {
	if key == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.paused[topic][key]
	return ok
}

// ResumeKey implements [pubsub.KeyResumer]: it accepts key on topic again
// after a failed publish.
func (p *Publisher) ResumeKey(topic, key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ks := p.paused[topic]; ks != nil {
		delete(ks, key)
		if len(ks) == 0 {
			delete(p.paused, topic)
		}
	}
}

// Close flushes buffered records; later Publish calls fail with
// [pubsub.ErrClosed]. It does not close the client. Safe to call more than
// once.
func (p *Publisher) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	return p.cl.Flush(ctx)
}

func recordID(r *kgo.Record) string {
	return fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset)
}
