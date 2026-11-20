package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// receiveN collects n deliveries without settling them.
func receiveN(t *testing.T, s *memory.Subscription, n int, wait time.Duration) []pubsub.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	var out []pubsub.Delivery
	_ = s.Receive(ctx, func(_ context.Context, d pubsub.Delivery) {
		out = append(out, d)
		if len(out) == n {
			cancel()
		}
	})
	return out
}

func TestKeyedDeliveryWaitsForSettlement(t *testing.T) {
	b := memory.New()
	s, err := b.Subscribe("t", "s")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := b.Publish(ctx, "t",
		&pubsub.Message{Key: "k", Data: []byte("1")},
		&pubsub.Message{Key: "k", Data: []byte("2")},
		&pubsub.Message{Data: []byte("free")}); err != nil {
		t.Fatal(err)
	}
	ds := receiveN(t, s, 3, 50*time.Millisecond)
	if len(ds) != 2 || string(ds[0].Message().Data) != "1" || string(ds[1].Message().Data) != "free" {
		t.Fatalf("got %d deliveries", len(ds))
	}
	ds[0].Nack()
	ds[1].Ack()
	ds = receiveN(t, s, 1, time.Second)
	if m := ds[0].Message(); string(m.Data) != "1" || m.DeliveryAttempt != 2 {
		t.Fatalf("redelivery = %q attempt %d", m.Data, m.DeliveryAttempt)
	}
	ds[0].Ack()
	ds = receiveN(t, s, 1, time.Second)
	if string(ds[0].Message().Data) != "2" {
		t.Fatal("key order broken")
	}
	ds[0].Ack()
	if s.Pending() != 0 {
		t.Fatalf("pending = %d", s.Pending())
	}
}

func TestClosedAndDuplicate(t *testing.T) {
	b := memory.New()
	if _, err := b.Subscribe("t", "s"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Subscribe("t", "s"); err == nil {
		t.Fatal("duplicate subscription accepted")
	}
	_ = b.Close(context.Background())
	if _, err := b.Publish(context.Background(), "t", &pubsub.Message{}); !errors.Is(err, pubsub.ErrClosed) {
		t.Fatalf("Publish after Close = %v", err)
	}
}
