package sqs

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/dungnh3/kitchen/pubsub"
)

// SourceOption configures a Source.
type SourceOption func(*Source)

// WithVisibilityTimeout sets the visibility timeout requested on receive; the
// Source extends it every half period while a message is in flight
// (default 30s).
func WithVisibilityTimeout(d time.Duration) SourceOption { return func(s *Source) { s.visibility = d } }

// WithWaitTime sets the long-poll wait (default 20s, the SQS maximum).
func WithWaitTime(d time.Duration) SourceOption { return func(s *Source) { s.wait = d } }

// WithPollers sets the number of concurrent ReceiveMessage loops (default 1).
// Order within a FIFO message group is preserved by SQS regardless.
func WithPollers(n int) SourceOption { return func(s *Source) { s.pollers = n } }

// WithAckFlushInterval sets how long acks are buffered before a
// DeleteMessageBatch (default 100ms; a full batch of 10 is sent at once).
func WithAckFlushInterval(d time.Duration) SourceOption { return func(s *Source) { s.ackFlush = d } }

// WithSourceLogger sets the logger (default slog.Default()).
func WithSourceLogger(l *slog.Logger) SourceOption { return func(s *Source) { s.log = l } }

// Source receives from one queue.
type Source struct {
	api        API
	queue      string
	visibility time.Duration
	wait       time.Duration
	pollers    int
	ackFlush   time.Duration
	log        *slog.Logger
}

var _ pubsub.Source = (*Source)(nil)

// NewSource returns a Source for queueURL.
func NewSource(api API, queueURL string, opts ...SourceOption) (*Source, error) {
	if api == nil || queueURL == "" {
		return nil, errors.New("sqs: nil client or empty queue URL")
	}
	s := &Source{api: api, queue: queueURL, visibility: 30 * time.Second, wait: 20 * time.Second, pollers: 1, ackFlush: 100 * time.Millisecond}
	for _, o := range opts {
		o(s)
	}
	if s.visibility < 2*time.Second || s.wait < 0 || s.wait > 20*time.Second || s.pollers < 1 || s.ackFlush <= 0 {
		return nil, errors.New("sqs: invalid source options")
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	return s, nil
}

// System implements [pubsub.Source].
func (s *Source) System() string { return System }

type inflight struct {
	mu      sync.Mutex
	handles map[string]struct{} // receipt handles held by the engine
	changed chan struct{}
}

func (f *inflight) add(h string) {
	f.mu.Lock()
	f.handles[h] = struct{}{}
	f.mu.Unlock()
}

func (f *inflight) remove(h string) {
	f.mu.Lock()
	delete(f.handles, h)
	f.mu.Unlock()
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

func (f *inflight) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Collect(maps.Keys(f.handles))
}

// Receive implements [pubsub.Source].
func (s *Source) Receive(ctx context.Context, fn func(context.Context, pubsub.Delivery)) error {
	inf := &inflight{handles: map[string]struct{}{}, changed: make(chan struct{}, 1)}
	acks := &ackSink{ch: make(chan string, 4*maxBatch)}
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		s.ackLoop(acks.ch)
	}()
	stopHB := make(chan struct{})
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		s.heartbeat(inf, stopHB)
	}()

	var wg sync.WaitGroup
	for range s.pollers {
		wg.Go(func() { s.poll(ctx, fn, inf, acks) })
	}
	wg.Wait()

	// Wait for the engine to settle what it holds (it drains concurrently),
	// then flush deletes and stop extending visibility.
	deadline := time.NewTimer(s.visibility)
	defer deadline.Stop()
	for len(inf.snapshot()) > 0 {
		select {
		case <-inf.changed:
			continue
		case <-deadline.C:
			s.log.Warn("sqs: messages still unsettled at shutdown; they become visible again")
		}
		break
	}
	close(stopHB)
	<-hbDone
	acks.close()
	<-ackDone
	return nil
}

// ackSink feeds the batched deleter; after Receive returns, acks are deleted
// one by one instead.
type ackSink struct {
	mu     sync.Mutex
	closed bool
	ch     chan string
}

func (a *ackSink) send(h string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return false
	}
	a.ch <- h
	return true
}

func (a *ackSink) close() {
	a.mu.Lock()
	a.closed = true
	close(a.ch)
	a.mu.Unlock()
}

func (s *Source) poll(ctx context.Context, fn func(context.Context, pubsub.Delivery), inf *inflight, acks *ackSink) {
	backoff := 100 * time.Millisecond
	for ctx.Err() == nil {
		out, err := s.api.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(s.queue),
			MaxNumberOfMessages:         maxBatch,
			WaitTimeSeconds:             int32(s.wait / time.Second),
			VisibilityTimeout:           int32(s.visibility / time.Second),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
			MessageAttributeNames:       []string{"All"},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Warn("sqs: receive failed", slog.String("queue", s.queue), slog.Any("error", err), slog.Duration("retry_in", backoff))
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		for _, m := range out.Messages {
			d, err := s.delivery(m, inf, acks)
			if err != nil {
				s.log.Error("sqs: undecodable message left for redrive", slog.String("messaging.message.id", aws.ToString(m.MessageId)), slog.Any("error", err))
				continue
			}
			inf.add(d.handle)
			fn(ctx, d)
		}
	}
}

func (s *Source) delivery(m types.Message, inf *inflight, acks *ackSink) (*delivery, error) {
	body := []byte(aws.ToString(m.Body))
	attrs := make(map[string]string, len(m.MessageAttributes))
	for k, v := range m.MessageAttributes {
		if v.StringValue != nil {
			attrs[k] = *v.StringValue
		}
	}
	if attrs[EncodingAttribute] == "base64" {
		b, err := base64.StdEncoding.DecodeString(string(body))
		if err != nil {
			return nil, err
		}
		body = b
		delete(attrs, EncodingAttribute)
	}
	attempt, _ := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	var sent time.Time
	if ms, err := strconv.ParseInt(m.Attributes[string(types.MessageSystemAttributeNameSentTimestamp)], 10, 64); err == nil {
		sent = time.UnixMilli(ms)
	}
	return &delivery{s: s, inf: inf, acks: acks, handle: aws.ToString(m.ReceiptHandle), msg: &pubsub.Message{
		ID: aws.ToString(m.MessageId), Topic: s.queue, Key: m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)],
		Data: body, Attributes: attrs, PublishTime: sent, DeliveryAttempt: attempt,
	}}, nil
}

type delivery struct {
	s      *Source
	inf    *inflight
	acks   *ackSink
	handle string
	msg    *pubsub.Message
	once   sync.Once
}

func (d *delivery) Message() *pubsub.Message { return d.msg }

// Ack queues the message for a batched delete.
func (d *delivery) Ack() {
	d.once.Do(func() {
		if !d.acks.send(d.handle) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := d.s.api.DeleteMessageBatch(ctx, &awssqs.DeleteMessageBatchInput{
				QueueUrl: aws.String(d.s.queue),
				Entries:  []types.DeleteMessageBatchRequestEntry{{Id: aws.String("0"), ReceiptHandle: aws.String(d.handle)}},
			}); err != nil {
				d.s.log.Warn("sqs: delete failed; message will be redelivered", slog.Any("error", err))
			}
		}
		d.inf.remove(d.handle)
	})
}

// Nack makes the message visible again immediately.
func (d *delivery) Nack() {
	d.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := d.s.api.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
			QueueUrl: aws.String(d.s.queue), ReceiptHandle: aws.String(d.handle), VisibilityTimeout: 0,
		}); err != nil {
			d.s.log.Warn("sqs: nack failed; message reappears after its visibility timeout", slog.Any("error", err))
		}
		d.inf.remove(d.handle)
	})
}

func (s *Source) ackLoop(acks <-chan string) {
	var buf []string
	t := time.NewTimer(s.ackFlush)
	defer t.Stop()
	flush := func() {
		for len(buf) > 0 {
			n := min(len(buf), maxBatch)
			entries := make([]types.DeleteMessageBatchRequestEntry, n)
			for i, h := range buf[:n] {
				entries[i] = types.DeleteMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(h)}
			}
			buf = buf[n:]
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			out, err := s.api.DeleteMessageBatch(ctx, &awssqs.DeleteMessageBatchInput{QueueUrl: aws.String(s.queue), Entries: entries})
			cancel()
			switch {
			case err != nil:
				s.log.Warn("sqs: delete failed; messages will be redelivered", slog.Int("count", n), slog.Any("error", err))
			case len(out.Failed) > 0:
				s.log.Warn("sqs: some deletes failed; messages will be redelivered", slog.Int("count", len(out.Failed)))
			}
		}
	}
	for {
		select {
		case h, ok := <-acks:
			if !ok {
				flush()
				return
			}
			buf = append(buf, h)
			if len(buf) >= maxBatch {
				flush()
			}
		case <-t.C:
			flush()
			t.Reset(s.ackFlush)
		}
	}
}

// heartbeat extends the visibility of in-flight messages every half period.
func (s *Source) heartbeat(inf *inflight, stop <-chan struct{}) {
	t := time.NewTicker(s.visibility / 2)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		handles := inf.snapshot()
		for start := 0; start < len(handles); start += maxBatch {
			chunk := handles[start:min(start+maxBatch, len(handles))]
			entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(chunk))
			for i, h := range chunk {
				entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
					Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(h), VisibilityTimeout: int32(s.visibility / time.Second),
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), s.visibility/2)
			if _, err := s.api.ChangeMessageVisibilityBatch(ctx, &awssqs.ChangeMessageVisibilityBatchInput{QueueUrl: aws.String(s.queue), Entries: entries}); err != nil {
				s.log.Warn("sqs: visibility extension failed", slog.Any("error", err))
			}
			cancel()
		}
	}
}
