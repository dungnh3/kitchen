package sqs

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/timex"
)

// SourceOption configures a Source.
type SourceOption func(*Source)

// WithVisibilityTimeout sets the visibility timeout requested on receive; the
// Source extends it every half period while a message is in flight, up to
// the SQS limit of 12 hours from receipt (default 30s, between 2s and 12h).
func WithVisibilityTimeout(d time.Duration) SourceOption { return func(s *Source) { s.visibility = d } }

// WithWaitTime sets the long-poll wait (default 20s, the SQS maximum).
func WithWaitTime(d time.Duration) SourceOption { return func(s *Source) { s.wait = d } }

// WithPollers sets the number of concurrent ReceiveMessage loops (default 1).
// Order within a FIFO message group is preserved by SQS regardless.
func WithPollers(n int) SourceOption { return func(s *Source) { s.pollers = n } }

// WithAckFlushInterval sets how long acks are buffered before a
// DeleteMessageBatch (default 100ms; a full batch of 10 is sent at once).
func WithAckFlushInterval(d time.Duration) SourceOption { return func(s *Source) { s.ackFlush = d } }

// WithAckConcurrency sets how many DeleteMessageBatch requests run at once
// (default 8, at least 1). Ack only queues the message; the queue holds 40
// acks per worker, and Ack blocks only while that queue is full.
func WithAckConcurrency(n int) SourceOption { return func(s *Source) { s.ackConcurrency = n } }

// WithSourceLogger sets the logger (nil means log.Default()).
func WithSourceLogger(l *log.Logger) SourceOption { return func(s *Source) { s.log = l } }

// Source receives from one queue.
type Source struct {
	api            API
	queue          string
	fifo           bool
	visibility     time.Duration
	wait           time.Duration
	pollers        int
	ackFlush       time.Duration
	ackConcurrency int
	log            *log.Logger

	maxReceive atomic.Int64 // maxReceiveCount of the redrive policy, 0 if unknown
}

var (
	_ pubsub.Source            = (*Source)(nil)
	_ pubsub.RedeliveryAdvisor = (*Source)(nil)
)

const (
	// maxVisibility is the SQS limit on a message's visibility, counted from
	// the receive that produced its receipt handle.
	maxVisibility = 12 * time.Hour
	// visibilityMargin is kept below the 12h limit: SQS starts the clock when
	// it hands out the message, slightly before this Source sees it.
	visibilityMargin = 5 * time.Second
	// callTimeout bounds DeleteMessageBatch, ChangeMessageVisibility and
	// GetQueueAttributes calls made outside the receive context.
	callTimeout = 10 * time.Second
	// deleteAttempts and extendAttempts bound retries of failed deletes and
	// visibility extensions.
	deleteAttempts = 3
	extendAttempts = 4
	// attemptIDTTL is how long a FIFO ReceiveRequestAttemptId is reused for
	// retries (SQS honours it for 5 minutes).
	attemptIDTTL = 4 * time.Minute
)

// NewSource returns a Source for queueURL.
func NewSource(api API, queueURL string, opts ...SourceOption) (*Source, error) {
	if api == nil || queueURL == "" {
		return nil, errors.New("sqs: nil client or empty queue URL")
	}
	s := &Source{
		api: api, queue: queueURL, fifo: isFIFO(queueURL),
		visibility: 30 * time.Second, wait: 20 * time.Second, pollers: 1,
		ackFlush: 100 * time.Millisecond, ackConcurrency: 8,
	}
	for _, o := range opts {
		o(s)
	}
	if s.visibility < 2*time.Second || s.visibility > maxVisibility || s.wait < 0 || s.wait > 20*time.Second ||
		s.pollers < 1 || s.ackFlush <= 0 || s.ackConcurrency < 1 {
		return nil, errors.New("sqs: invalid source options")
	}
	s.log = log.OrDefault(s.log)
	return s, nil
}

// System implements [pubsub.Source].
func (s *Source) System() string { return System }

// Redelivery implements [pubsub.RedeliveryAdvisor]. SQS holds back the rest
// of a FIFO message group while a message is in flight and returns the nacked
// message first, so no key fence is needed (standard queues have no keys).
// MaxDeliveryAttempts is the queue's redrive maxReceiveCount, read when
// Receive starts, or 0 when there is none or it cannot be read.
func (s *Source) Redelivery() pubsub.Redelivery {
	return pubsub.Redelivery{MaxDeliveryAttempts: int(s.maxReceive.Load())}
}

// readRedrive loads maxReceiveCount from the queue's redrive policy.
func (s *Source) readRedrive(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	out, err := s.api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(s.queue),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedrivePolicy},
	})
	if err != nil {
		s.log.WarnContext(ctx, "sqs: cannot read the redrive policy; max delivery attempts unknown", log.String("queue", s.queue), log.Err(err))
		s.maxReceive.Store(0)
		return
	}
	n, err := parseMaxReceiveCount(out.Attributes[string(types.QueueAttributeNameRedrivePolicy)])
	if err != nil {
		s.log.WarnContext(ctx, "sqs: unreadable redrive policy; max delivery attempts unknown", log.String("queue", s.queue), log.Err(err))
	}
	s.maxReceive.Store(int64(n))
}

// parseMaxReceiveCount returns maxReceiveCount of a RedrivePolicy JSON value
// (a string or a number), or 0 for an empty policy.
func parseMaxReceiveCount(policy string) (int, error) {
	if policy == "" {
		return 0, nil
	}
	var p struct {
		MaxReceiveCount json.RawMessage `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(policy), &p); err != nil {
		return 0, err
	}
	if len(p.MaxReceiveCount) == 0 {
		return 0, nil
	}
	n, err := strconv.Atoi(strings.Trim(string(p.MaxReceiveCount), `"`))
	if err != nil || n < 0 {
		return 0, fmt.Errorf("sqs: maxReceiveCount %s is not a count", p.MaxReceiveCount)
	}
	return n, nil
}

// Receive implements [pubsub.Source]. It returns nil when ctx is done, or an
// UNAVAILABLE error wrapping the SQS error when the queue cannot be read at
// all (it does not exist, access is denied, the URL is invalid). Transient
// receive errors are retried with backoff. Before returning, it keeps
// extending the visibility of every message the engine still holds until
// all are settled, then flushes pending deletes.
func (s *Source) Receive(ctx context.Context, fn func(context.Context, pubsub.Delivery)) error {
	s.readRedrive(ctx)
	inf := newInflight()
	acks := s.startAcker() //nolint:contextcheck // deletes must outlive ctx until in-flight messages settle
	stopHB := make(chan struct{})
	hbDone := make(chan struct{})
	go func() { //nolint:contextcheck // lease extensions must outlive ctx until in-flight messages settle
		defer close(hbDone)
		s.heartbeat(inf, stopHB)
	}()

	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		fatalOnce sync.Once
		fatal     error
		wg        sync.WaitGroup
	)
	for range s.pollers {
		wg.Go(func() {
			if err := s.poll(pctx, fn, inf, acks); err != nil {
				fatalOnce.Do(func() { fatal = err; cancel() })
			}
		})
	}
	wg.Wait()

	// The engine settles everything it holds before its Run returns; keep
	// extending visibility until it has, then flush deletes.
	inf.waitEmpty()
	close(stopHB)
	<-hbDone
	acks.close()
	return fatal
}

func (s *Source) poll(ctx context.Context, fn func(context.Context, pubsub.Delivery), inf *inflight, acks *acker) error {
	failures := 0
	var attemptID string
	var attemptAt time.Time
	for ctx.Err() == nil {
		in := &awssqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(s.queue),
			MaxNumberOfMessages:         maxBatch,
			WaitTimeSeconds:             seconds(s.wait),
			VisibilityTimeout:           seconds(s.visibility),
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
			MessageAttributeNames:       []string{"All"},
		}
		if s.fifo {
			// A retry of a failed receive reuses the id, so SQS returns the
			// messages the lost response held instead of hiding them for a
			// visibility timeout.
			if attemptID == "" || time.Since(attemptAt) > attemptIDTTL {
				attemptID, attemptAt = rand.Text(), time.Now()
			}
			in.ReceiveRequestAttemptId = aws.String(attemptID)
		}
		out, err := s.api.ReceiveMessage(ctx, in)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if fatalReceiveError(err) {
				s.log.ErrorContext(ctx, "sqs: receive failed permanently", log.String("queue", s.queue), log.Err(err))
				return errs.Wrap(fmt.Errorf("sqs: receive from %s: %w", s.queue, err), errs.ErrDependencyUnavailable)
			}
			failures++
			backoff := timex.Backoff(100*time.Millisecond, 30*time.Second, failures)
			s.log.WarnContext(ctx, "sqs: receive failed", log.String("queue", s.queue), log.Err(err), log.Duration("retry_in", backoff))
			t := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil
			case <-t.C:
			}
			continue
		}
		attemptID = ""
		failures = 0
		ds := make([]*delivery, 0, len(out.Messages))
		for _, m := range out.Messages {
			d, err := s.delivery(m, inf, acks)
			if err != nil {
				s.log.ErrorContext(ctx, "sqs: undecodable message left for redrive", log.String("messaging.message.id", aws.ToString(m.MessageId)), log.Err(err))
				continue
			}
			ds = append(ds, d)
		}
		// Every message of the response is extended from now on, including
		// the ones waiting while fn blocks for an earlier one.
		inf.add(ds, time.Now())
		for _, d := range ds {
			fn(ctx, d)
		}
	}
	return nil
}

// fatalReceiveError reports whether err means the queue cannot be read at
// all, so retrying is pointless.
func fatalReceiveError(err error) bool {
	var qne *types.QueueDoesNotExist
	var ia *types.InvalidAddress
	var is *types.InvalidSecurity
	if errors.As(err, &qne) || errors.As(err, &ia) || errors.As(err, &is) {
		return true
	}
	var coded interface{ ErrorCode() string }
	if !errors.As(err, &coded) {
		return false
	}
	switch coded.ErrorCode() {
	case "QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue",
		"AccessDenied", "AccessDeniedException", "InvalidAddress", "InvalidSecurity",
		"InvalidClientTokenId", "UnrecognizedClientException":
		return true
	}
	return false
}

func (s *Source) delivery(m types.Message, inf *inflight, acks *acker) (*delivery, error) {
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
	key := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
	if key != "" && !s.fifo {
		// Standard queue: the group is a fair-queue tenant, not an order.
		attrs[GroupIDAttribute] = key
		key = ""
	}
	return &delivery{s: s, inf: inf, acks: acks, handle: aws.ToString(m.ReceiptHandle), msg: &pubsub.Message{
		ID: aws.ToString(m.MessageId), Topic: s.queue, Key: key,
		Data: body, Attributes: attrs, PublishTime: sent, DeliveryAttempt: attempt,
	}}, nil
}

type delivery struct {
	s      *Source
	inf    *inflight
	acks   *acker
	handle string
	msg    *pubsub.Message
	once   sync.Once
}

func (d *delivery) Message() *pubsub.Message { return d.msg }

// Ack queues the message for a batched delete; after Receive has returned
// it deletes the message directly.
func (d *delivery) Ack() {
	d.once.Do(func() {
		it := ackItem{handle: d.handle, id: d.msg.ID}
		if !d.acks.send(it) {
			d.s.deleteBatch([]ackItem{it})
		}
		d.inf.remove(d.handle)
	})
}

// Nack makes the message visible again immediately.
func (d *delivery) Nack() {
	d.once.Do(func() {
		d.inf.nacking(d.handle)
		ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
		defer cancel()
		if _, err := d.s.api.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
			QueueUrl: aws.String(d.s.queue), ReceiptHandle: aws.String(d.handle), VisibilityTimeout: 0,
		}); err != nil {
			d.s.log.Warn("sqs: nack failed; message reappears after its visibility timeout",
				log.String("messaging.message.id", d.msg.ID), log.Err(err))
		}
		d.inf.remove(d.handle)
	})
}

// inflight tracks the receipt handles the Source has received and not yet
// settled, for the heartbeat.
type inflight struct {
	mu      sync.Mutex
	msgs    map[string]*held
	changed chan struct{}
}

type held struct {
	id        string
	received  time.Time
	nacked    bool // settled or being nacked: never extend again
	settled   bool // settled while extending: dropped when the request ends
	noExtend  bool // reached the 12h limit or SQS refused an extension
	extending bool // part of a heartbeat request in flight
	renack    bool // nacked while extending: undo the extension afterwards
}

func newInflight() *inflight {
	return &inflight{msgs: map[string]*held{}, changed: make(chan struct{}, 1)}
}

func (f *inflight) add(ds []*delivery, now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, d := range ds {
		f.msgs[d.handle] = &held{id: d.msg.ID, received: now}
	}
}

// nacking marks h as being nacked so the heartbeat stops extending it; if an
// extension of h is in flight, the heartbeat makes h visible again after it.
func (f *inflight) nacking(h string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m := f.msgs[h]; m != nil {
		m.nacked = true
		m.renack = m.extending
	}
}

// remove drops a settled handle (after the heartbeat request holding it, if
// any, has ended).
func (f *inflight) remove(h string) {
	f.mu.Lock()
	if m := f.msgs[h]; m != nil {
		if m.extending {
			m.nacked, m.settled = true, true
		} else {
			delete(f.msgs, h)
		}
	}
	f.mu.Unlock()
	f.signal()
}

func (f *inflight) signal() {
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

func (f *inflight) len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.msgs)
}

// waitEmpty blocks until every received message is settled. The heartbeat
// must still be running: it drops handles settled during its requests.
func (f *inflight) waitEmpty() {
	for f.len() > 0 {
		<-f.changed
	}
}

type extension struct {
	handle, id string
	secs       int32
}

// beginExtend returns the extensions due now and marks them in flight.
// Messages that just reached the 12h limit are returned in capped, once.
func (f *inflight) beginExtend(now time.Time, visibility time.Duration) (due []extension, capped []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for h, m := range f.msgs {
		if m.nacked || m.noExtend {
			continue
		}
		left := maxVisibility - visibilityMargin - now.Sub(m.received)
		if left < time.Second {
			m.noExtend = true
			capped = append(capped, m.id)
			continue
		}
		m.extending = true
		due = append(due, extension{handle: h, id: m.id, secs: seconds(min(visibility, left))})
	}
	return due, capped
}

// endExtend ends the heartbeat request for chunk: it stops extending refused
// handles, drops handles settled meanwhile and returns those nacked meanwhile.
func (f *inflight) endExtend(chunk []extension, refused map[string]bool) (renack []string) {
	f.mu.Lock()
	for _, e := range chunk {
		m := f.msgs[e.handle]
		if m == nil {
			continue
		}
		m.extending = false
		if refused[e.handle] {
			m.noExtend = true
		}
		if m.renack {
			m.renack = false
			renack = append(renack, e.handle)
		}
		if m.settled {
			delete(f.msgs, e.handle)
		}
	}
	f.mu.Unlock()
	f.signal()
	return renack
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
		due, capped := inf.beginExtend(time.Now(), s.visibility)
		for _, id := range capped {
			s.log.Warn("sqs: visibility reached the 12h SQS limit; the message becomes visible again if not settled",
				log.String("queue", s.queue), log.String("messaging.message.id", id))
		}
		for start := 0; start < len(due); start += maxBatch {
			s.extend(inf, due[start:min(start+maxBatch, len(due))], stop)
		}
	}
}

// extend sends one ChangeMessageVisibilityBatch for chunk, retrying failed
// entries that SQS did not blame on the sender, then undoes the extension of
// messages nacked meanwhile.
func (s *Source) extend(inf *inflight, chunk []extension, stop <-chan struct{}) {
	refused := map[string]bool{}
	pending := chunk
	for attempt := 1; len(pending) > 0; attempt++ {
		entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(pending))
		for i, e := range pending {
			entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
				Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(e.handle), VisibilityTimeout: e.secs,
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), min(max(s.visibility/4, time.Second), callTimeout))
		out, err := s.api.ChangeMessageVisibilityBatch(ctx, &awssqs.ChangeMessageVisibilityBatchInput{QueueUrl: aws.String(s.queue), Entries: entries})
		cancel()
		var retry []extension
		if err != nil {
			retry = pending
		} else {
			for _, fe := range out.Failed {
				i, convErr := strconv.Atoi(aws.ToString(fe.Id))
				if convErr != nil || i < 0 || i >= len(pending) {
					continue
				}
				e := pending[i]
				if fe.SenderFault {
					refused[e.handle] = true
					s.log.Warn("sqs: visibility extension refused; no longer extending the message",
						log.String("queue", s.queue), log.String("messaging.message.id", e.id),
						log.String("code", aws.ToString(fe.Code)), log.String("reason", aws.ToString(fe.Message)))
					continue
				}
				retry = append(retry, e)
			}
		}
		if len(retry) == 0 {
			break
		}
		if attempt == extendAttempts {
			s.log.Warn("sqs: visibility extension failed; messages may become visible again",
				log.String("queue", s.queue), log.Int("count", len(retry)), log.Err(err))
			break
		}
		t := time.NewTimer(timex.Backoff(s.visibility/40, s.visibility/10, attempt))
		select {
		case <-stop:
			t.Stop()
			retry = nil
		case <-t.C:
		}
		pending = retry
	}
	if renack := inf.endExtend(chunk, refused); len(renack) > 0 {
		s.undoExtension(renack)
	}
}

// undoExtension makes nacked messages visible again after a heartbeat that
// raced with their Nack.
func (s *Source) undoExtension(handles []string) {
	for start := 0; start < len(handles); start += maxBatch {
		chunk := handles[start:min(start+maxBatch, len(handles))]
		entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(chunk))
		for i, h := range chunk {
			entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(h), VisibilityTimeout: 0}
		}
		ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
		if _, err := s.api.ChangeMessageVisibilityBatch(ctx, &awssqs.ChangeMessageVisibilityBatchInput{QueueUrl: aws.String(s.queue), Entries: entries}); err != nil {
			s.log.Warn("sqs: nack after a concurrent extension failed; messages reappear after their visibility timeout",
				log.String("queue", s.queue), log.Int("count", len(chunk)), log.Err(err))
		}
		cancel()
	}
}

// seconds converts a validated duration (at most maxVisibility) to the
// whole seconds the SQS API takes.
func seconds(d time.Duration) int32 {
	s := int64(d / time.Second)
	if s > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(s) //nolint:gosec // G115: s <= MaxInt32 (checked above)
}
