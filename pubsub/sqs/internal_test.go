package sqs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
)

// deleteRecorder records DeleteMessageBatch calls; other methods are unused.
type deleteRecorder struct {
	API
	mu      sync.Mutex
	handles []string
}

func (r *deleteRecorder) DeleteMessageBatch(_ context.Context, in *awssqs.DeleteMessageBatchInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageBatchOutput, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range in.Entries {
		r.handles = append(r.handles, aws.ToString(e.ReceiptHandle))
	}
	return &awssqs.DeleteMessageBatchOutput{}, nil
}

// An Ack after the batched deleter has stopped deletes the message directly.
func TestAckAfterAckerClosedDeletesDirectly(t *testing.T) {
	api := &deleteRecorder{}
	s, err := NewSource(api, "https://sqs/q", WithSourceLogger(log.Discard()))
	if err != nil {
		t.Fatal(err)
	}
	acks := s.startAcker()
	acks.close()
	inf := newInflight()
	d := &delivery{s: s, inf: inf, acks: acks, handle: "h1", msg: &pubsub.Message{ID: "m1"}}
	inf.add([]*delivery{d}, time.Now())
	d.Ack()
	if len(api.handles) != 1 || api.handles[0] != "h1" || inf.len() != 0 {
		t.Fatalf("deleted %v, in flight %d; want h1 deleted directly", api.handles, inf.len())
	}
}
