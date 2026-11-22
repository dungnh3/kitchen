package sqs_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/sqs"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var quiet = log.Discard()

// fakeSQS is an in-memory queue with SQS visibility and FIFO group semantics.
type fakeSQS struct {
	mu      sync.Mutex
	fifo    bool
	seq     int
	msgs    []*fmsg
	changed chan struct{}
	// failSends makes the next n SendMessageBatch requests fail.
	failSends int

	// Failure injection and recording. Hooks run without f.mu held, so they
	// may block.
	// receiveHook runs before each ReceiveMessage; an error fails the call.
	receiveHook func(ctx context.Context, in *awssqs.ReceiveMessageInput) error
	// deleteHook runs before each DeleteMessageBatch and returns the entries
	// to fail (they are not deleted), or an error to fail the request.
	deleteHook func(entries []types.DeleteMessageBatchRequestEntry) ([]types.BatchResultErrorEntry, error)
	// cmvbHook runs before each ChangeMessageVisibilityBatch and returns the
	// entries to fail (not applied), or an error to fail the request.
	cmvbHook func(in *awssqs.ChangeMessageVisibilityBatchInput) ([]types.BatchResultErrorEntry, error)
	// sendFailed fails entries of SendMessageBatch whose body it returns
	// non-nil for.
	sendFailed func(body string) *types.BatchResultErrorEntry
	// redrive is the RedrivePolicy queue attribute; attrErr fails
	// GetQueueAttributes.
	redrive string
	attrErr error
	// visRejected counts visibility changes SQS would reject for exceeding
	// 12 hours from receipt.
	visRejected int
}

type fmsg struct {
	id, body, group, handle string
	attrs                   map[string]types.MessageAttributeValue
	visibleAt               time.Time
	receivedAt              time.Time
	receives                int
}

// maxRequest is the SQS limit on a message and on a SendMessageBatch total
// (bodies plus attribute names, types and values).
const maxRequest = 1 << 20

func entrySize(e types.SendMessageBatchRequestEntry) int {
	n := len(aws.ToString(e.MessageBody))
	for k, v := range e.MessageAttributes {
		n += len(k) + len(aws.ToString(v.DataType)) + len(aws.ToString(v.StringValue)) + len(v.BinaryValue)
	}
	return n
}

// sqsAllowed reports whether body uses only characters SQS accepts.
func sqsAllowed(body string) bool {
	for _, c := range body {
		allowed := c == 0x9 || c == 0xA || c == 0xD || (c >= 0x20 && c <= 0xD7FF) || (c >= 0xE000 && c <= 0xFFFD) || c >= 0x10000
		if !allowed {
			return false
		}
	}
	return utf8.ValidString(body)
}

func newFake(fifo bool) *fakeSQS { return &fakeSQS{fifo: fifo, changed: make(chan struct{}, 1)} }

func (f *fakeSQS) notify() {
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

func (f *fakeSQS) len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.msgs)
}

func (f *fakeSQS) SendMessageBatch(_ context.Context, in *awssqs.SendMessageBatchInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failSends > 0 {
		f.failSends--
		return nil, errors.New("service unavailable")
	}
	total := 0
	for _, e := range in.Entries {
		total += entrySize(e)
	}
	if total > maxRequest {
		return nil, &types.BatchRequestTooLong{Message: aws.String(fmt.Sprintf("%d bytes", total))}
	}
	out := &awssqs.SendMessageBatchOutput{}
	for _, e := range in.Entries {
		if !sqsAllowed(aws.ToString(e.MessageBody)) {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String("InvalidMessageContents"), Message: aws.String("invalid characters"), SenderFault: true})
			continue
		}
		if f.sendFailed != nil {
			if fe := f.sendFailed(aws.ToString(e.MessageBody)); fe != nil {
				fe.Id = e.Id
				out.Failed = append(out.Failed, *fe)
				continue
			}
		}
		if aws.ToString(e.MessageBody) == "reject" {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String("InvalidMessageContents"), Message: aws.String("rejected")})
			continue
		}
		f.seq++
		id := fmt.Sprint("m", f.seq)
		f.msgs = append(f.msgs, &fmsg{id: id, body: aws.ToString(e.MessageBody), group: aws.ToString(e.MessageGroupId), attrs: e.MessageAttributes})
		out.Successful = append(out.Successful, types.SendMessageBatchResultEntry{Id: e.Id, MessageId: aws.String(id)})
	}
	f.notify()
	return out, nil
}

func (f *fakeSQS) ReceiveMessage(ctx context.Context, in *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	if f.receiveHook != nil {
		if err := f.receiveHook(ctx, in); err != nil {
			return nil, err
		}
	}
	deadline := time.NewTimer(time.Duration(in.WaitTimeSeconds) * time.Second)
	defer deadline.Stop()
	for {
		if out := f.take(in); len(out.Messages) > 0 {
			return out, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return &awssqs.ReceiveMessageOutput{}, nil
		case <-f.changed:
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (f *fakeSQS) take(in *awssqs.ReceiveMessageInput) *awssqs.ReceiveMessageOutput {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	blocked := map[string]bool{}
	out := &awssqs.ReceiveMessageOutput{}
	for _, m := range f.msgs {
		if len(out.Messages) == int(in.MaxNumberOfMessages) {
			break
		}
		visible := !m.visibleAt.After(now)
		if f.fifo && (blocked[m.group] || !visible) {
			blocked[m.group] = true // later group messages wait for this one
			if visible {
				continue
			}
			continue
		}
		if !visible {
			continue
		}
		m.receives++
		m.handle = fmt.Sprintf("%s-%d", m.id, m.receives)
		m.visibleAt = now.Add(time.Duration(in.VisibilityTimeout) * time.Second)
		m.receivedAt = now
		attrs := map[string]string{
			"ApproximateReceiveCount": strconv.Itoa(m.receives),
			"SentTimestamp":           strconv.FormatInt(now.UnixMilli(), 10),
		}
		if m.group != "" {
			attrs["MessageGroupId"] = m.group
		}
		out.Messages = append(out.Messages, types.Message{
			MessageId: aws.String(m.id), ReceiptHandle: aws.String(m.handle), Body: aws.String(m.body),
			Attributes: attrs, MessageAttributes: m.attrs,
		})
	}
	return out
}

func (f *fakeSQS) DeleteMessageBatch(_ context.Context, in *awssqs.DeleteMessageBatchInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageBatchOutput, error) {
	out := &awssqs.DeleteMessageBatchOutput{}
	skip := map[string]bool{}
	if f.deleteHook != nil {
		failed, err := f.deleteHook(in.Entries)
		if err != nil {
			return nil, err
		}
		out.Failed = failed
		for _, fe := range failed {
			skip[aws.ToString(fe.Id)] = true
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range in.Entries {
		if skip[aws.ToString(e.Id)] {
			continue
		}
		f.msgs = slices.DeleteFunc(f.msgs, func(m *fmsg) bool { return m.handle == aws.ToString(e.ReceiptHandle) })
		out.Successful = append(out.Successful, types.DeleteMessageBatchResultEntry{Id: e.Id})
	}
	f.notify()
	return out, nil
}

// setVisibility applies a visibility change and reports false when SQS
// would reject it for exceeding 12 hours from receipt.
func (f *fakeSQS) setVisibility(handle string, secs int32) bool {
	for _, m := range f.msgs {
		if m.handle == handle {
			now := time.Now()
			if now.Sub(m.receivedAt)+time.Duration(secs)*time.Second > 12*time.Hour {
				f.visRejected++
				return false
			}
			m.visibleAt = now.Add(time.Duration(secs) * time.Second)
		}
	}
	return true
}

func (f *fakeSQS) ChangeMessageVisibility(_ context.Context, in *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	f.setVisibility(aws.ToString(in.ReceiptHandle), in.VisibilityTimeout)
	f.mu.Unlock()
	f.notify()
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibilityBatch(_ context.Context, in *awssqs.ChangeMessageVisibilityBatchInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityBatchOutput, error) {
	out := &awssqs.ChangeMessageVisibilityBatchOutput{}
	skip := map[string]bool{}
	if f.cmvbHook != nil {
		failed, err := f.cmvbHook(in)
		if err != nil {
			return nil, err
		}
		out.Failed = failed
		for _, fe := range failed {
			skip[aws.ToString(fe.Id)] = true
		}
	}
	f.mu.Lock()
	for _, e := range in.Entries {
		if skip[aws.ToString(e.Id)] {
			continue
		}
		if !f.setVisibility(aws.ToString(e.ReceiptHandle), e.VisibilityTimeout) {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String("InvalidParameterValue"), Message: aws.String("exceeds 12 hours"), SenderFault: true})
			continue
		}
		out.Successful = append(out.Successful, types.ChangeMessageVisibilityBatchResultEntry{Id: e.Id})
	}
	f.mu.Unlock()
	return out, nil
}

func (f *fakeSQS) GetQueueAttributes(_ context.Context, _ *awssqs.GetQueueAttributesInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueAttributesOutput, error) {
	if f.attrErr != nil {
		return nil, f.attrErr
	}
	out := &awssqs.GetQueueAttributesOutput{Attributes: map[string]string{}}
	if f.redrive != "" {
		out.Attributes[string(types.QueueAttributeNameRedrivePolicy)] = f.redrive
	}
	return out, nil
}

func run(t *testing.T, f *fakeSQS, queue string, h pubsub.Handler, until func() bool) {
	t.Helper()
	src, err := sqs.NewSource(f, queue, sqs.WithWaitTime(time.Second), sqs.WithVisibilityTimeout(2*time.Second),
		sqs.WithAckFlushInterval(5*time.Millisecond), sqs.WithSourceLogger(quiet))
	if err != nil {
		t.Fatal(err)
	}
	s, err := pubsub.NewSubscriber(src, h, pubsub.WithLogger(quiet), pubsub.WithRetry(0, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- s.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for !until() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRoundTripBinaryAndAttributes(t *testing.T) {
	f := newFake(false)
	pub, _ := sqs.NewPublisher(f)
	bin := []byte{0xff, 0x00, 0xfe}
	ids, err := pub.Publish(context.Background(), "https://sqs/q",
		&pubsub.Message{Data: []byte("héllo"), Attributes: map[string]string{"a": "1"}},
		&pubsub.Message{Data: bin})
	if err != nil || ids[0] == "" || ids[1] == "" {
		t.Fatalf("publish: %v %v", ids, err)
	}
	var mu sync.Mutex
	got := map[string]*pubsub.Message{}
	run(t, f, "https://sqs/q", func(_ context.Context, m *pubsub.Message) error {
		mu.Lock()
		got[m.ID] = m
		mu.Unlock()
		return nil
	}, func() bool { return f.len() == 0 })
	if f.len() != 0 {
		t.Fatalf("%d messages not deleted", f.len())
	}
	if m := got[ids[0]]; string(m.Data) != "héllo" || m.Attributes["a"] != "1" || m.DeliveryAttempt != 1 || m.Topic != "https://sqs/q" {
		t.Fatalf("text message = %+v", m)
	}
	if m := got[ids[1]]; !slices.Equal(m.Data, bin) || m.Attributes[sqs.EncodingAttribute] != "" {
		t.Fatalf("binary message = %+v", m)
	}
}

func TestFIFOOrderWithNack(t *testing.T) {
	f := newFake(true)
	pub, _ := sqs.NewPublisher(f)
	var msgs []*pubsub.Message
	for i := range 12 {
		msgs = append(msgs, &pubsub.Message{Key: fmt.Sprint("g", i%2), Data: []byte(strconv.Itoa(i))})
	}
	if _, err := pub.Publish(context.Background(), "https://sqs/q.fifo", msgs...); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	got := map[string][]int{}
	failed := false
	run(t, f, "https://sqs/q.fifo", func(_ context.Context, m *pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		n, _ := strconv.Atoi(string(m.Data))
		if n == 2 && !failed {
			failed = true
			return errors.New("transient")
		}
		if n == 2 && m.DeliveryAttempt != 2 {
			t.Errorf("attempt = %d", m.DeliveryAttempt)
		}
		got[m.Key] = append(got[m.Key], n)
		return nil
	}, func() bool { return f.len() == 0 })
	mu.Lock()
	defer mu.Unlock()
	for k, seq := range got {
		if len(seq) != 6 || !slices.IsSorted(seq) {
			t.Fatalf("group %s = %v", k, seq)
		}
	}
}

func TestPublishErrors(t *testing.T) {
	f := newFake(true)
	pub, _ := sqs.NewPublisher(f)
	many := map[string]string{}
	for i := range 11 {
		many[fmt.Sprint("k", i)] = "v"
	}
	ids, err := pub.Publish(context.Background(), "https://sqs/q.fifo",
		&pubsub.Message{Key: "g", Data: []byte("ok")},
		&pubsub.Message{Data: []byte("no group")},
		&pubsub.Message{Key: "g", Data: []byte("x"), Attributes: many},
		&pubsub.Message{Key: "g", Data: []byte("reject")},
	)
	var pe *pubsub.PublishError
	if !errors.As(err, &pe) || len(pe.Failed) != 3 || ids[0] == "" {
		t.Fatalf("err = %v ids = %v", err, ids)
	}
}

func TestFIFOKeyPausedAfterFailedPublish(t *testing.T) {
	const q = "https://sqs/q.fifo"
	f := newFake(true)
	pub, _ := sqs.NewPublisher(f)
	ctx := context.Background()

	// A failed request pauses its keys for the rest of the call.
	f.failSends = 1
	var msgs []*pubsub.Message
	for i := range 12 {
		msgs = append(msgs, &pubsub.Message{Key: "g", Data: []byte(fmt.Sprint(i))})
	}
	msgs = append(msgs, &pubsub.Message{Key: "h", Data: []byte("other")})
	ids, err := pub.Publish(ctx, q, msgs...)
	var pe *pubsub.PublishError
	if !errors.As(err, &pe) || len(pe.Failed) != 12 {
		t.Fatalf("err = %v", err)
	}
	for _, i := range []int{10, 11} {
		if !errors.Is(pe.Failed[i], pubsub.ErrKeyPaused) {
			t.Fatalf("message %d: %v, want ErrKeyPaused", i, pe.Failed[i])
		}
	}
	if ids[12] == "" {
		t.Fatal("other key was blocked")
	}
	// ... and in later calls, until ResumeKey.
	if _, err := pub.Publish(ctx, q, &pubsub.Message{Key: "g", Data: []byte("later")}); !errors.Is(err, pubsub.ErrKeyPaused) {
		t.Fatalf("later publish err = %v, want ErrKeyPaused", err)
	}
	pub.ResumeKey(q, "g")
	if _, err := pub.Publish(ctx, q, &pubsub.Message{Key: "g", Data: []byte("0")}); err != nil {
		t.Fatalf("after resume: %v", err)
	}
	if f.len() != 2 {
		t.Fatalf("queue has %d messages, want 2", f.len())
	}
}

func TestStandardQueueFairGroup(t *testing.T) {
	const q = "https://sqs/q"
	f := newFake(false)
	pub, _ := sqs.NewPublisher(f)
	if _, err := pub.Publish(context.Background(), q, &pubsub.Message{Key: "tenant-1", Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	group := f.msgs[0].group
	f.mu.Unlock()
	if group != "tenant-1" {
		t.Fatalf("MessageGroupId = %q, want tenant-1", group)
	}
	var mu sync.Mutex
	var got *pubsub.Message
	run(t, f, q, func(_ context.Context, m *pubsub.Message) error {
		mu.Lock()
		got = m
		mu.Unlock()
		return nil
	}, func() bool { mu.Lock(); defer mu.Unlock(); return got != nil })
	mu.Lock()
	defer mu.Unlock()
	if got == nil || got.Key != "" || got.Attributes[sqs.GroupIDAttribute] != "tenant-1" {
		t.Fatalf("received %+v", got)
	}
}
