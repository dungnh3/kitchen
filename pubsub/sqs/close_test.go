package sqs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/sqs"
)

const testQueue = "https://sqs.us-east-1.amazonaws.com/1/q"

// Publish after Close fails with pubsub.ErrClosed and sends nothing.
func TestPublisherPublishAfterClose(t *testing.T) {
	f := newFake(false)
	p, err := sqs.NewPublisher(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(t.Context()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_, err = p.Publish(t.Context(), testQueue, &pubsub.Message{Data: []byte("x")})
	if !errors.Is(err, pubsub.ErrClosed) {
		t.Fatalf("Publish after Close: err = %v, want ErrClosed", err)
	}
	if n := f.len(); n != 0 {
		t.Fatalf("%d messages sent after Close", n)
	}
}

// blockingSQS holds SendMessageBatch until release is closed.
type blockingSQS struct {
	*fakeSQS
	started, release chan struct{}
}

func (b *blockingSQS) SendMessageBatch(ctx context.Context, in *awssqs.SendMessageBatchInput, o ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error) {
	close(b.started)
	<-b.release
	return b.fakeSQS.SendMessageBatch(ctx, in, o...)
}

// Close waits for a Publish that is already sending, and gives up when its
// ctx is done.
func TestPublisherCloseWaitsForInFlightPublish(t *testing.T) {
	b := &blockingSQS{fakeSQS: newFake(false), started: make(chan struct{}), release: make(chan struct{})}
	p, err := sqs.NewPublisher(b)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := p.Publish(context.Background(), testQueue, &pubsub.Message{Data: []byte("x")})
		done <- err
	}()
	<-b.started

	short, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := p.Close(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close with an in-flight Publish: err = %v, want DeadlineExceeded", err)
	}

	closed := make(chan error, 1)
	go func() { closed <- p.Close(t.Context()) }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned %v before the in-flight Publish finished", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(b.release)
	if err := <-done; err != nil {
		t.Fatalf("in-flight Publish: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := b.len(); n != 1 {
		t.Fatalf("sent %d messages, want 1", n)
	}
}
