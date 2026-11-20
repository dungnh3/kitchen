// Package sqs implements [pubsub.Publisher] and [pubsub.Source] on Amazon SQS
// using github.com/aws/aws-sdk-go-v2/service/sqs.
//
// The topic is the queue URL. On FIFO queues (URL ending in ".fifo"),
// Message.Key is the MessageGroupId, so messages of one key are delivered in
// order; it is required there and ignored on standard queues. A deduplication
// ID can be given in the [DedupIDAttribute] attribute (otherwise enable
// content-based deduplication on the queue).
//
// Attributes become SQS message attributes (at most 10 per message, counting
// the trace context). SQS bodies must be text: a payload that is not valid
// UTF-8 is sent base64-encoded with the [EncodingAttribute] attribute and
// decoded on receive.
//
// Dead-lettering: configure a redrive policy (maxReceiveCount) on the queue.
// Message.DeliveryAttempt is ApproximateReceiveCount.
package sqs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/dungnh3/kitchen/pubsub"
)

// System is the OpenTelemetry messaging.system value.
const System = "aws_sqs"

const (
	// DedupIDAttribute, when set on a published message, is used as the FIFO
	// MessageDeduplicationId and is not sent as a message attribute.
	DedupIDAttribute = "sqs-dedup-id"
	// EncodingAttribute marks a base64-encoded body ("base64").
	EncodingAttribute = "kitchen-body-encoding"

	maxBatch      = 10
	maxAttributes = 10
)

// API is the subset of *sqs.Client used by this package.
type API interface {
	SendMessageBatch(ctx context.Context, in *awssqs.SendMessageBatchInput, opts ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error)
	ReceiveMessage(ctx context.Context, in *awssqs.ReceiveMessageInput, opts ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, in *awssqs.DeleteMessageBatchInput, opts ...func(*awssqs.Options)) (*awssqs.DeleteMessageBatchOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *awssqs.ChangeMessageVisibilityInput, opts ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
	ChangeMessageVisibilityBatch(ctx context.Context, in *awssqs.ChangeMessageVisibilityBatchInput, opts ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityBatchOutput, error)
}

var _ API = (*awssqs.Client)(nil)

// PublisherOption configures a Publisher.
type PublisherOption func(*Publisher)

// WithPublisherTelemetry configures tracing and metrics.
func WithPublisherTelemetry(opts ...pubsub.TelemetryOption) PublisherOption {
	return func(p *Publisher) { p.telOpts = append(p.telOpts, opts...) }
}

// Publisher sends messages with SendMessageBatch (10 per request).
type Publisher struct {
	api     API
	telOpts []pubsub.TelemetryOption
	tel     *pubsub.Telemetry
}

var _ pubsub.Publisher = (*Publisher)(nil)

// NewPublisher returns a Publisher using api (usually *sqs.Client).
func NewPublisher(api API, opts ...PublisherOption) (*Publisher, error) {
	if api == nil {
		return nil, errors.New("sqs: nil client")
	}
	p := &Publisher{api: api}
	for _, o := range opts {
		o(p)
	}
	p.tel = pubsub.NewTelemetry(System, p.telOpts...)
	return p, nil
}

func isFIFO(queueURL string) bool { return strings.HasSuffix(queueURL, ".fifo") }

func entry(i int, m *pubsub.Message, fifo bool) (types.SendMessageBatchRequestEntry, error) {
	e := types.SendMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i))}
	body := string(m.Data)
	attrs := map[string]types.MessageAttributeValue{}
	if !utf8.Valid(m.Data) {
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
		return e, fmt.Errorf("sqs: %d attributes exceed the SQS limit of %d", len(attrs), maxAttributes)
	}
	if len(attrs) > 0 {
		e.MessageAttributes = attrs
	}
	if fifo {
		if m.Key == "" {
			return e, errors.New("sqs: FIFO queues require Message.Key (MessageGroupId)")
		}
		e.MessageGroupId = aws.String(m.Key)
	}
	return e, nil
}

// Publish implements [pubsub.Publisher]. topic is the queue URL.
func (p *Publisher) Publish(ctx context.Context, topic string, msgs ...*pubsub.Message) ([]string, error) {
	return p.tel.Publish(ctx, topic, msgs, func(ctx context.Context, msgs []*pubsub.Message) ([]string, error) {
		if topic == "" {
			return nil, errors.New("sqs: empty queue URL")
		}
		fifo := isFIFO(topic)
		ids := make([]string, len(msgs))
		failed := map[int]error{}
		for start := 0; start < len(msgs); start += maxBatch {
			end := min(start+maxBatch, len(msgs))
			var entries []types.SendMessageBatchRequestEntry
			for i := start; i < end; i++ {
				e, err := entry(i, msgs[i], fifo)
				if err != nil {
					failed[i] = err
					continue
				}
				entries = append(entries, e)
			}
			if len(entries) == 0 {
				continue
			}
			out, err := p.api.SendMessageBatch(ctx, &awssqs.SendMessageBatchInput{QueueUrl: aws.String(topic), Entries: entries})
			if err != nil {
				for _, e := range entries {
					i, _ := strconv.Atoi(*e.Id)
					failed[i] = err
				}
				continue
			}
			for _, s := range out.Successful {
				i, _ := strconv.Atoi(aws.ToString(s.Id))
				ids[i] = aws.ToString(s.MessageId)
			}
			for _, f := range out.Failed {
				i, _ := strconv.Atoi(aws.ToString(f.Id))
				failed[i] = fmt.Errorf("sqs: %s: %s", aws.ToString(f.Code), aws.ToString(f.Message))
			}
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

// Close is a no-op: requests are not buffered.
func (p *Publisher) Close(context.Context) error { return nil }
