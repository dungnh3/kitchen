package sqs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/sqs"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeSQS is an in-memory queue with SQS visibility and FIFO group semantics.
type fakeSQS struct {
	mu      sync.Mutex
	fifo    bool
	seq     int
	msgs    []*fmsg
	changed chan struct{}
}

type fmsg struct {
	id, body, group, handle string
	attrs                   map[string]types.MessageAttributeValue
	visibleAt               time.Time
	receives                int
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
	out := &awssqs.SendMessageBatchOutput{}
	for _, e := range in.Entries {
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
		if int32(len(out.Messages)) == in.MaxNumberOfMessages {
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
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range in.Entries {
		f.msgs = slices.DeleteFunc(f.msgs, func(m *fmsg) bool { return m.handle == aws.ToString(e.ReceiptHandle) })
	}
	f.notify()
	return &awssqs.DeleteMessageBatchOutput{}, nil
}

func (f *fakeSQS) setVisibility(handle string, secs int32) {
	for _, m := range f.msgs {
		if m.handle == handle {
			m.visibleAt = time.Now().Add(time.Duration(secs) * time.Second)
		}
	}
}

func (f *fakeSQS) ChangeMessageVisibility(_ context.Context, in *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	f.setVisibility(aws.ToString(in.ReceiptHandle), in.VisibilityTimeout)
	f.mu.Unlock()
	f.notify()
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQS) ChangeMessageVisibilityBatch(_ context.Context, in *awssqs.ChangeMessageVisibilityBatchInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityBatchOutput, error) {
	f.mu.Lock()
	for _, e := range in.Entries {
		f.setVisibility(aws.ToString(e.ReceiptHandle), e.VisibilityTimeout)
	}
	f.mu.Unlock()
	return &awssqs.ChangeMessageVisibilityBatchOutput{}, nil
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
	run(t, f, "https://sqs/q", func(ctx context.Context, m *pubsub.Message) error {
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
	run(t, f, "https://sqs/q.fifo", func(ctx context.Context, m *pubsub.Message) error {
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
