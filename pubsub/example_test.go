package pubsub_test

import (
	"context"
	"fmt"
	"time"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
)

func ExampleNewSubscriber() {
	broker := memory.New()
	sub, _ := broker.Subscribe("orders", "billing")

	type Order struct {
		ID    string
		Total int
	}
	done := make(chan struct{})
	s, err := pubsub.NewSubscriber(sub, pubsub.JSON(func(_ context.Context, o Order, _ *pubsub.Message) error {
		fmt.Println("order", o.ID, o.Total)
		close(done)
		return nil // ack; return an error to retry, pubsub.Permanent(err) to dead-letter
	}), pubsub.WithConcurrency(32))
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error)
	go func() { stopped <- s.Run(ctx) }()

	m, _ := pubsub.NewJSON(Order{ID: "o-1", Total: 42})
	m.Key = "customer-7" // same key => handled in order
	if _, err := broker.Publish(context.Background(), "orders", m); err != nil {
		panic(err)
	}
	<-done
	cancel()
	fmt.Println("run:", <-stopped)
	// Output:
	// order o-1 42
	// run: <nil>
}

func ExampleNewBatchSubscriber() {
	broker := memory.New()
	sub, _ := broker.Subscribe("events", "warehouse")

	done := make(chan struct{})
	s, _ := pubsub.NewBatchSubscriber(sub, func(_ context.Context, ms []*pubsub.Message) error {
		fmt.Println("batch of", len(ms))
		close(done)
		// Fail only message 1; the others are acked:
		// return &pubsub.BatchError{Failed: map[int]error{1: err}}
		return nil
	}, pubsub.WithBatch(3, time.Second), pubsub.WithConcurrency(1))

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error)
	go func() { stopped <- s.Run(ctx) }()
	_, _ = broker.Publish(ctx, "events",
		&pubsub.Message{Data: []byte("a")}, &pubsub.Message{Data: []byte("b")}, &pubsub.Message{Data: []byte("c")})
	<-done
	cancel()
	if err := <-stopped; err != nil {
		panic(err)
	}
	// Output: batch of 3
}
