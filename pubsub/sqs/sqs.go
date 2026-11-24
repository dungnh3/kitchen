// Package sqs implements [pubsub.Publisher] and [pubsub.Source] on Amazon SQS
// using github.com/aws/aws-sdk-go-v2/service/sqs.
//
// The topic is the queue URL. On FIFO queues (URL ending in ".fifo"),
// Message.Key is the MessageGroupId, so messages of one key are delivered in
// order; it is required there. A deduplication ID can be given in the
// [DedupIDAttribute] attribute (otherwise enable content-based deduplication
// on the queue).
//
// FIFO ordering on publish: when a keyed message fails to publish, its key is
// paused: later messages of that key, in the same call and in later calls,
// fail with an error wrapping [pubsub.ErrKeyPaused] until
// [Publisher.ResumeKey], so a retry cannot land after newer messages. Messages
// of one key sent in the same SendMessageBatch request as the failed one may
// still have been accepted by SQS.
//
// Standard queues: a non-empty Message.Key is sent as MessageGroupId, which
// SQS uses as the tenant of a fair queue (no ordering). On receive, standard
// queues leave Message.Key empty (so the Subscriber does not serialize what
// SQS does not order) and expose the group in the [GroupIDAttribute]
// attribute.
//
// Attributes become SQS message attributes (at most 10 per message, counting
// the trace context). SQS bodies must be text in the XML character set
// (#x9 | #xA | #xD | #x20-#xD7FF | #xE000-#xFFFD | #x10000-#x10FFFF): any
// other payload (binary, protobuf, text with NUL or control characters) is
// sent base64-encoded with the [EncodingAttribute] attribute and decoded on
// receive.
//
// Size: a message (body plus attribute names, types and values) and a
// SendMessageBatch request are each limited to 1 MiB; Publish splits
// requests to fit, and a single message over the limit fails on its own.
//
// Errors: a message SQS or this package rejects as invalid (too large, too
// many attributes, a SenderFault batch entry) fails with an error wrapping
// [pubsub.ErrPermanent] and errs INVALID_ARGUMENT, so retrying it is
// pointless; broker and transport failures are errs UNAVAILABLE
// (errs.ErrDependencyUnavailable).
//
// Dead-lettering: configure a redrive policy (maxReceiveCount) on the queue.
// Message.DeliveryAttempt is ApproximateReceiveCount; the Source reports
// maxReceiveCount through [pubsub.RedeliveryAdvisor].
package sqs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/pubsub"
)

// System is the OpenTelemetry messaging.system value.
const System = "aws_sqs"

const (
	// DedupIDAttribute is the attribute that, when set on a published message,
	// is used as the FIFO MessageDeduplicationId and not sent as an attribute.
	DedupIDAttribute = "sqs-dedup-id"
	// EncodingAttribute marks a base64-encoded body ("base64").
	EncodingAttribute = "kitchen-body-encoding"
	// GroupIDAttribute carries the MessageGroupId (fair-queue tenant) of a
	// message received from a standard queue.
	GroupIDAttribute = "sqs-message-group-id"

	maxBatch      = 10
	maxAttributes = 10
	// maxMessageBytes and maxBatchBytes are the SQS limits on one message
	// (body plus attribute names, types and values) and on the sum of the
	// messages of one SendMessageBatch.
	maxMessageBytes = 1 << 20
	maxBatchBytes   = 1 << 20
)

// API is the subset of *sqs.Client used by this package.
type API interface {
	SendMessageBatch(ctx context.Context, in *awssqs.SendMessageBatchInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error)
	ReceiveMessage(ctx context.Context, in *awssqs.ReceiveMessageInput, opts ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, in *awssqs.DeleteMessageBatchInput, opts ...func(*awssqs.Options)) (*awssqs.DeleteMessageBatchOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *awssqs.ChangeMessageVisibilityInput, opts ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
	ChangeMessageVisibilityBatch(ctx context.Context, in *awssqs.ChangeMessageVisibilityBatchInput, opts ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityBatchOutput, error)
	GetQueueAttributes(ctx context.Context, in *awssqs.GetQueueAttributesInput, opts ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error)
}

var _ API = (*awssqs.Client)(nil)

// PublisherOption configures a Publisher.
type PublisherOption func(*Publisher)

// WithPublisherTelemetry configures tracing and metrics.
func WithPublisherTelemetry(opts ...pubsub.TelemetryOption) PublisherOption {
	return func(p *Publisher) { p.telOpts = append(p.telOpts, opts...) }
}

// Publisher sends messages with SendMessageBatch (up to 10 per request,
// within the 1 MiB request limit).
type Publisher struct {
	api     API
	telOpts []pubsub.TelemetryOption
	tel     *pubsub.Telemetry

	mu       sync.Mutex
	paused   map[string]map[string]struct{} // queue URL -> paused FIFO keys
	closed   bool
	inflight sync.WaitGroup // Publish calls past the closed check
}

var (
	_ pubsub.Publisher  = (*Publisher)(nil)
	_ pubsub.KeyResumer = (*Publisher)(nil)
)

// NewPublisher returns a Publisher using api (usually *sqs.Client).
func NewPublisher(api API, opts ...PublisherOption) (*Publisher, error) {
	if api == nil {
		return nil, errors.New("sqs: nil client")
	}
	p := &Publisher{api: api, paused: map[string]map[string]struct{}{}}
	for _, o := range opts {
		o(p)
	}
	p.tel = pubsub.NewTelemetry(System, p.telOpts...)
	return p, nil
}

func isFIFO(queueURL string) bool { return strings.HasSuffix(queueURL, ".fifo") }

// sqsText reports whether b is UTF-8 text made only of characters SQS
// accepts in a message body.
func sqsText(b []byte) bool {
	for len(b) > 0 {
		r, n := utf8.DecodeRune(b)
		if r == utf8.RuneError && n <= 1 {
			return false
		}
		if r != 0x9 && r != 0xA && r != 0xD && (r < 0x20 || r > 0xD7FF) && (r < 0xE000 || r > 0xFFFD) && r < 0x10000 {
			return false
		}
		b = b[n:]
	}
	return true
}

// invalid marks a message SQS can never accept.
func invalid(err error) error { return pubsub.Permanent(errs.Wrap(err, errs.ErrInvalidArgument)) }

// entry builds the batch entry of m and returns its size as SQS counts it.
func entry(i int, m *pubsub.Message, fifo bool) (types.SendMessageBatchRequestEntry, int, error) {
	e := types.SendMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i))}
	body := string(m.Data)
	attrs := map[string]types.MessageAttributeValue{}
	if !sqsText(m.Data) {
		body = base64.StdEncoding.EncodeToString(m.Data)
		attrs[EncodingAttribute] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("base64")}
	}
	e.MessageBody = aws.String(body)
	for k, v := range m.Attributes {
		if k == DedupIDAttribute {
			if fifo {
				e.MessageDeduplicationId = aws.String(v)
			}
			continue
		}
		attrs[k] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
	}
	if len(attrs) > maxAttributes {
		return e, 0, invalid(fmt.Errorf("sqs: %d attributes exceed the SQS limit of %d", len(attrs), maxAttributes))
	}
	size := len(body)
	for k, v := range attrs {
		size += len(k) + len(aws.ToString(v.DataType)) + len(aws.ToString(v.StringValue))
	}
	if size > maxMessageBytes {
		return e, 0, invalid(fmt.Errorf("sqs: message of %d bytes (body and attributes) exceeds the SQS limit of %d", size, maxMessageBytes))
	}
	if len(attrs) > 0 {
		e.MessageAttributes = attrs
	}
	if fifo && m.Key == "" {
		return e, 0, invalid(errors.New("sqs: FIFO queues require Message.Key (MessageGroupId)"))
	}
	if m.Key != "" {
		e.MessageGroupId = aws.String(m.Key)
	}
	return e, size, nil
}

// requestError classifies a failed SendMessageBatch request: context errors
// stay as they are, anything else is a broker failure.
func requestError(topic string, err error) error {
	err = fmt.Errorf("sqs: send to %s: %w", topic, err)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errs.Wrap(err, errs.ErrDependencyUnavailable)
}

// entryError classifies a failed batch entry by SenderFault.
func entryError(f types.BatchResultErrorEntry) error {
	err := fmt.Errorf("sqs: %s: %s", aws.ToString(f.Code), aws.ToString(f.Message))
	if f.SenderFault {
		return invalid(err)
	}
	return errs.Wrap(err, errs.ErrDependencyUnavailable)
}

// Publish implements [pubsub.Publisher]. topic is the queue URL.
func (p *Publisher) Publish(ctx context.Context, topic string, msgs ...*pubsub.Message) ([]string, error) {
	return p.tel.Publish(ctx, topic, msgs, func(ctx context.Context, msgs []*pubsub.Message) ([]string, error) {
		if topic == "" {
			return nil, errs.Wrap(errors.New("sqs: empty queue URL"), errs.ErrInvalidArgument)
		}
		if !p.begin() {
			return nil, fmt.Errorf("sqs: publish to %s: %w", topic, pubsub.ErrClosed)
		}
		defer p.inflight.Done()
		fifo := isFIFO(topic)
		ids := make([]string, len(msgs))
		failed := map[int]error{}
		fail := func(i int, err error) {
			failed[i] = err
			if fifo {
				p.pause(topic, msgs[i].Key)
			}
		}
		var entries []types.SendMessageBatchRequestEntry
		size := 0
		send := func() {
			if len(entries) == 0 {
				return
			}
			out, err := p.api.SendMessageBatch(ctx, &awssqs.SendMessageBatchInput{QueueUrl: aws.String(topic), Entries: entries})
			if err != nil {
				err = requestError(topic, err)
				for _, e := range entries {
					i, _ := strconv.Atoi(*e.Id)
					fail(i, err)
				}
			} else {
				for _, s := range out.Successful {
					i, _ := strconv.Atoi(aws.ToString(s.Id))
					ids[i] = aws.ToString(s.MessageId)
				}
				for _, f := range out.Failed {
					i, _ := strconv.Atoi(aws.ToString(f.Id))
					fail(i, entryError(f))
				}
			}
			entries, size = entries[:0], 0
		}
		for i, m := range msgs {
			if fifo && p.isPaused(topic, m.Key) {
				failed[i] = fmt.Errorf("sqs: queue %s group %q: %w", topic, m.Key, pubsub.ErrKeyPaused)
				continue
			}
			e, n, err := entry(i, m, fifo)
			if err != nil {
				fail(i, err)
				continue
			}
			if size+n > maxBatchBytes {
				send()
				if fifo && p.isPaused(topic, m.Key) { // that request failed for this key
					failed[i] = fmt.Errorf("sqs: queue %s group %q: %w", topic, m.Key, pubsub.ErrKeyPaused)
					continue
				}
			}
			entries = append(entries, e)
			size += n
			if len(entries) == maxBatch {
				send()
			}
		}
		send()
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

func (p *Publisher) pause(queue, key string) {
	if key == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused[queue] == nil {
		p.paused[queue] = map[string]struct{}{}
	}
	p.paused[queue][key] = struct{}{}
}

func (p *Publisher) isPaused(queue, key string) bool {
	if key == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.paused[queue][key]
	return ok
}

// ResumeKey implements [pubsub.KeyResumer]: it accepts key on the FIFO queue
// topic again after a failed publish.
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

// begin registers a Publish call, or reports false after Close.
func (p *Publisher) begin() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.inflight.Add(1)
	return true
}

// Close makes later Publish calls fail with [pubsub.ErrClosed] and waits
// for the Publish calls already sending, or until ctx is done. Nothing is
// buffered, so there is nothing else to flush. It does not close the API
// client. Safe to call more than once.
func (p *Publisher) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	done := make(chan struct{})
	go func() {
		p.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
