// Package gcp implements [pubsub.Publisher] and [pubsub.Source] on Google
// Cloud Pub/Sub using cloud.google.com/go/pubsub/v2.
//
// Ordering: the Publisher enables message ordering on every topic handle, so
// messages with a Key are delivered in order to subscriptions that have
// message ordering enabled. A failed keyed publish pauses its key: later
// publishes with that key fail with an error wrapping [pubsub.ErrKeyPaused]
// until [Publisher.ResumeKey]. Keys are never resumed automatically, which
// keeps FIFO order.
//
// Dead-lettering: configure a dead-letter policy (maxDeliveryAttempts) on the
// subscription; nacked messages are then moved by Pub/Sub and
// Message.DeliveryAttempt is populated.
package gcp

import (
	"context"
	"errors"
	"fmt"
	"sync"

	gpubsub "cloud.google.com/go/pubsub/v2"

	"github.com/dungnh3/kitchen/pubsub"
)

// System is the OpenTelemetry messaging.system value.
const System = "gcp_pubsub"

// PublisherOption configures a Publisher.
type PublisherOption func(*Publisher)

// WithPublishSettings sets the batching settings applied to every topic
// handle (default gpubsub.DefaultPublishSettings).
func WithPublishSettings(s gpubsub.PublishSettings) PublisherOption {
	return func(p *Publisher) { p.settings = &s }
}

// WithPublisherTelemetry configures tracing and metrics.
func WithPublisherTelemetry(opts ...pubsub.TelemetryOption) PublisherOption {
	return func(p *Publisher) { p.telOpts = append(p.telOpts, opts...) }
}

// Publisher publishes to Pub/Sub topics. Topic handles are created on first
// use and reused; Close flushes and stops them. Topics may be given as IDs or
// full names ("projects/p/topics/t").
type Publisher struct {
	client   *gpubsub.Client
	settings *gpubsub.PublishSettings
	telOpts  []pubsub.TelemetryOption
	tel      *pubsub.Telemetry

	mu     sync.Mutex
	topics map[string]*gpubsub.Publisher
	closed bool
}

var (
	_ pubsub.Publisher  = (*Publisher)(nil)
	_ pubsub.KeyResumer = (*Publisher)(nil)
)

// NewPublisher returns a Publisher using client, which the caller keeps owning
// (Close does not close it).
func NewPublisher(client *gpubsub.Client, opts ...PublisherOption) (*Publisher, error) {
	if client == nil {
		return nil, errors.New("gcp: nil client")
	}
	p := &Publisher{client: client, topics: map[string]*gpubsub.Publisher{}}
	for _, o := range opts {
		o(p)
	}
	p.tel = pubsub.NewTelemetry(System, p.telOpts...)
	return p, nil
}

func (p *Publisher) topic(name string) (*gpubsub.Publisher, error) {
	if name == "" {
		return nil, errors.New("gcp: empty topic")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, pubsub.ErrClosed
	}
	if t, ok := p.topics[name]; ok {
		return t, nil
	}
	t := p.client.Publisher(name)
	if p.settings != nil {
		t.PublishSettings = *p.settings
	}
	// Must be set before the first Publish; it only affects keyed messages.
	t.EnableMessageOrdering = true
	p.topics[name] = t
	return t, nil
}

// Publish implements [pubsub.Publisher]. All messages are handed to the
// client's batcher before waiting, so one call is sent in as few RPCs as the
// publish settings allow.
func (p *Publisher) Publish(ctx context.Context, topic string, msgs ...*pubsub.Message) ([]string, error) {
	return p.tel.Publish(ctx, topic, msgs, func(ctx context.Context, msgs []*pubsub.Message) ([]string, error) {
		t, err := p.topic(topic)
		if err != nil {
			return nil, err
		}
		results := make([]*gpubsub.PublishResult, len(msgs))
		for i, m := range msgs {
			results[i] = t.Publish(ctx, &gpubsub.Message{Data: m.Data, Attributes: m.Attributes, OrderingKey: m.Key})
		}
		ids := make([]string, len(msgs))
		failed := map[int]error{}
		for i, r := range results {
			id, err := r.Get(ctx)
			if err != nil {
				var paused gpubsub.ErrPublishingPaused
				if errors.As(err, &paused) {
					err = fmt.Errorf("gcp: topic %s key %q: %w", topic, paused.OrderingKey, pubsub.ErrKeyPaused)
				}
				failed[i] = err
				continue
			}
			ids[i] = id
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

// ResumeKey implements [pubsub.KeyResumer].
func (p *Publisher) ResumeKey(topic, key string) {
	if key == "" {
		return
	}
	p.mu.Lock()
	t := p.topics[topic]
	p.mu.Unlock()
	if t != nil {
		t.ResumePublish(key)
	}
}

// Close flushes and stops every topic handle. It returns ctx.Err() if ctx
// ends first (the handles keep flushing in the background).
func (p *Publisher) Close(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	topics := make([]*gpubsub.Publisher, 0, len(p.topics))
	for _, t := range p.topics {
		topics = append(topics, t)
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, t := range topics {
			wg.Go(t.Stop)
		}
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
