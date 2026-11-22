// Package gcp implements [pubsub.Publisher] and [pubsub.Source] on Google
// Cloud Pub/Sub using cloud.google.com/go/pubsub/v2.
//
// Ordering: the Publisher enables message ordering on every topic handle, so
// messages with a Key are delivered in order to subscriptions that have
// message ordering enabled. A failed keyed publish pauses its key: its own
// error and the errors of later publishes with that key wrap
// [pubsub.ErrKeyPaused] until [Publisher.ResumeKey]. The error of the failed
// publish itself also wraps its cause, reported as
// errs.ErrDependencyUnavailable unless it is a context error, so it can be
// told apart from a publish rejected because the key was already paused.
// Keys are never resumed automatically, which keeps FIFO order. Because
// ordering is always enabled, publish hedging (HedgingSettings) is not
// supported.
//
// Subscriptions without message ordering: the Source delivers messages with
// an empty Key, since Pub/Sub does not order them and serialising or fencing
// them per key would only cost throughput; the ordering key stays readable
// in the [OrderingKeyAttribute] attribute.
//
// Ordering on receive: [pubsub.Subscriber] fences a key after one of its
// messages is nacked and nacks the key's other messages until Pub/Sub
// redelivers that message, so within one process B never runs before the
// redelivered A. Across pods, ordering-key affinity keeps a key on one client
// while it has outstanding messages, but affinity is not guaranteed for
// StreamingPull, and a fence lives in one process. Make handlers of ordered
// subscriptions tolerate a rare reorder, e.g. by checking a version or
// sequence number stored with the data.
//
// Dead-lettering: configure a dead-letter policy (maxDeliveryAttempts) on the
// subscription; nacked messages are then moved by Pub/Sub. Pub/Sub sets
// Message.DeliveryAttempt only on subscriptions with a dead-letter policy;
// without one it is 0, so pubsub.WithMaxDeliveryAttempts has no effect. The
// Source reports the policy's maxDeliveryAttempts to the Subscriber (see
// [Source.Redelivery]), which then never fences a key on the nack that sends
// a message to the dead-letter topic.
package gcp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	gpubsub "cloud.google.com/go/pubsub/v2"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/pubsub"
)

// System is the OpenTelemetry messaging.system value.
const System = "gcp_pubsub"

// PublisherOption configures a Publisher.
type PublisherOption func(*Publisher)

// WithPublishSettings sets the batching settings applied to every topic
// handle (default gpubsub.DefaultPublishSettings). Zero DelayThreshold,
// CountThreshold, ByteThreshold, Timeout and CompressionBytesThreshold take
// the value from gpubsub.DefaultPublishSettings, so a partially filled struct
// keeps batching and a publish deadline. HedgingSettings must be nil: v2 does
// not allow hedging with message ordering, which the Publisher enables, so
// NewPublisher rejects it.
func WithPublishSettings(s gpubsub.PublishSettings) PublisherOption {
	return func(p *Publisher) {
		d := gpubsub.DefaultPublishSettings
		if s.DelayThreshold == 0 {
			s.DelayThreshold = d.DelayThreshold
		}
		if s.CountThreshold == 0 {
			s.CountThreshold = d.CountThreshold
		}
		if s.ByteThreshold == 0 {
			s.ByteThreshold = d.ByteThreshold
		}
		if s.Timeout == 0 {
			s.Timeout = d.Timeout
		}
		if s.CompressionBytesThreshold == 0 {
			s.CompressionBytesThreshold = d.CompressionBytesThreshold
		}
		if s.FlowControlSettings.MaxOutstandingMessages == 0 {
			s.FlowControlSettings.MaxOutstandingMessages = d.FlowControlSettings.MaxOutstandingMessages
		}
		if s.FlowControlSettings.MaxOutstandingBytes == 0 {
			s.FlowControlSettings.MaxOutstandingBytes = d.FlowControlSettings.MaxOutstandingBytes
		}
		p.settings = &s
	}
}

// WithPublisherTelemetry configures tracing and metrics.
func WithPublisherTelemetry(opts ...pubsub.TelemetryOption) PublisherOption {
	return func(p *Publisher) { p.telOpts = append(p.telOpts, opts...) }
}

// Publisher publishes to Pub/Sub topics. Topic handles are created on first
// use and reused; Close flushes and stops them. Topics may be given as IDs or
// full names ("projects/p/topics/t"); both spellings of a topic share one
// handle, so ordering and paused keys are per topic, whichever is used.
type Publisher struct {
	client   *gpubsub.Client
	settings *gpubsub.PublishSettings
	telOpts  []pubsub.TelemetryOption
	tel      *pubsub.Telemetry

	mu      sync.Mutex
	topics  map[string]*gpubsub.Publisher // by full topic name
	aliases map[string]string             // caller's spelling -> full name
	closed  bool
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
	p := &Publisher{client: client, topics: map[string]*gpubsub.Publisher{}, aliases: map[string]string{}}
	for _, o := range opts {
		o(p)
	}
	if p.settings != nil && p.settings.HedgingSettings != nil {
		return nil, errs.Wrap(errors.New("gcp: HedgingSettings is not supported: the Publisher enables message ordering, "+
			"which the Pub/Sub client does not allow together with hedging"), errs.ErrInvalidArgument)
	}
	p.tel = pubsub.NewTelemetry(System, p.telOpts...)
	return p, nil
}

func (p *Publisher) topic(name string) (*gpubsub.Publisher, error) {
	if name == "" {
		return nil, errs.Wrap(errors.New("gcp: empty topic"), errs.ErrInvalidArgument)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, pubsub.ErrClosed
	}
	full, ok := p.aliases[name]
	if ok {
		return p.topics[full], nil
	}
	t := p.client.Publisher(name)
	full = t.String()
	p.aliases[name] = full
	if cached, ok := p.topics[full]; ok {
		return cached, nil
	}
	if p.settings != nil {
		t.PublishSettings = *p.settings
	}
	// Must be set before the first Publish; it only affects keyed messages.
	t.EnableMessageOrdering = true
	p.topics[full] = t
	return t, nil
}

// fullTopicName returns the full name of topic as the client resolves it.
func (p *Publisher) fullTopicName(topic string) string {
	if len(strings.Split(topic, "/")) == 4 {
		return topic
	}
	return fmt.Sprintf("projects/%s/topics/%s", p.client.Project(), topic)
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
				failed[i] = publishError(topic, msgs[i].Key, r, err)
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

// publishError maps the error of one message's publish result.
func publishError(topic, key string, r *gpubsub.PublishResult, err error) error {
	var paused gpubsub.ErrPublishingPaused
	switch {
	case errors.As(err, &paused):
		return fmt.Errorf("gcp: topic %s key %q: %w", topic, paused.OrderingKey, pubsub.ErrKeyPaused)
	case errors.Is(err, gpubsub.ErrPublisherStopped):
		// A Publish that raced Close: the handle was stopped under it.
		return fmt.Errorf("gcp: topic %s: %w", topic, pubsub.ErrClosed)
	case key == "" || !isReady(r.Ready()):
		// Unkeyed, or the caller's ctx ended before the result: the publish
		// may still succeed and the key is not known to be paused.
		return err
	default:
		// The client pauses a key on any failure of its publish (flow control
		// or RPC), so the caller must handle it and call ResumeKey.
		return newKeyPausedError(topic, key, err)
	}
}

func isReady(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// keyPausedError is the failure that paused a key. It wraps the cause (a
// broker failure as errs.ErrDependencyUnavailable) and pubsub.ErrKeyPaused.
type keyPausedError struct {
	topic, key string
	cause      error
}

func newKeyPausedError(topic, key string, cause error) *keyPausedError {
	_, isErrs := errors.AsType[*errs.Error](cause)
	if !isErrs && !errors.Is(cause, context.Canceled) && !errors.Is(cause, context.DeadlineExceeded) {
		cause = errs.Wrap(cause, errs.ErrDependencyUnavailable)
	}
	return &keyPausedError{topic: topic, key: key, cause: cause}
}

func (e *keyPausedError) Error() string {
	return fmt.Sprintf("gcp: topic %s key %q paused after a failed publish: %v", e.topic, e.key, e.cause)
}

func (e *keyPausedError) Unwrap() []error { return []error{e.cause, pubsub.ErrKeyPaused} }

// ResumeKey implements [pubsub.KeyResumer]. topic may be an ID or a full
// name.
func (p *Publisher) ResumeKey(topic, key string) {
	if key == "" {
		return
	}
	p.mu.Lock()
	t := p.topics[p.fullTopicName(topic)]
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
