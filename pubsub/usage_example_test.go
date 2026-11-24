package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
)

// Messages with the same Key are handled one at a time, in publish order;
// different keys run in parallel up to WithConcurrency.
func Example_ordering() {
	broker := memory.New()
	sub, _ := broker.Subscribe("accounts", "projector")

	var mu sync.Mutex
	var got []string
	done := make(chan struct{})
	s, _ := pubsub.NewSubscriber(sub, func(_ context.Context, m *pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		if got = append(got, string(m.Data)); len(got) == 3 {
			close(done)
		}
		return nil
	},
		pubsub.WithConcurrency(16),               // different keys in parallel
		pubsub.WithKeyFence(pubsub.KeyFenceAuto), // the default: fence a failed key when the broker needs it
	)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error)
	go func() { stopped <- s.Run(ctx) }()

	_, _ = broker.Publish(ctx, "accounts",
		&pubsub.Message{Key: "acct-1", Data: []byte("opened")},
		&pubsub.Message{Key: "acct-1", Data: []byte("credited")},
		&pubsub.Message{Key: "acct-1", Data: []byte("closed")})
	<-done
	cancel()
	if err := <-stopped; err != nil {
		panic(err)
	}
	fmt.Println(got)
	// Output: [opened credited closed]
}

// A handler error is retried in process (WithRetry), then nacked so the
// broker redelivers. pubsub.Permanent skips the retries and goes straight to
// the dead-letter hook; so does a message past WithMaxDeliveryAttempts.
func ExampleWithDeadLetter() {
	logger := log.Discard() // in main: logger, _ := log.Setup(opts)
	broker := memory.New()
	sub, _ := broker.Subscribe("payments", "ledger")

	done := make(chan struct{})
	s, err := pubsub.NewSubscriber(sub, func(_ context.Context, m *pubsub.Message) error {
		if len(m.Data) == 0 {
			return pubsub.Permanent(errors.New("empty payload")) // retrying cannot fix it
		}
		return nil
	},
		pubsub.WithRetry(3, 100*time.Millisecond, 5*time.Second),
		pubsub.WithMaxDeliveryAttempts(5),
		pubsub.WithDeadLetter(func(_ context.Context, m *pubsub.Message, cause error) error {
			fmt.Println("dead letter:", m.Topic, cause)
			close(done)
			return nil // acked; return an error to nack it instead
		}),
		pubsub.WithLogger(logger),
	)
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error)
	go func() { stopped <- s.Run(ctx) }()

	_, _ = broker.Publish(ctx, "payments", &pubsub.Message{})
	<-done
	cancel()
	if err := <-stopped; err != nil {
		panic(err)
	}
	// Output: dead letter: payments permanent: empty payload
}

// Publish errors carry kitchen/errs codes: ErrKeyPaused is
// FailedPrecondition, ErrClosed is Unavailable, bad input is
// InvalidArgument, a broker status that rejects the request (for example
// InvalidArgument or NotFound) keeps its code, and any other broker failure
// is errs.ErrDependencyUnavailable (503).
func Example_errors() {
	broker := memory.New()
	_ = broker.Close(context.Background())

	_, err := broker.Publish(context.Background(), "orders", &pubsub.Message{Data: []byte("x")})
	fmt.Println(errors.Is(err, pubsub.ErrClosed), errs.Code(err))
	// Output: true Unavailable
}

// Close stops a Subscriber started with go s.Run(ctx) and waits for the
// drain, for code that shuts components down one by one.
func ExampleSubscriber_Close() {
	broker := memory.New()
	sub, _ := broker.Subscribe("orders", "billing")
	s, _ := pubsub.NewSubscriber(sub, func(context.Context, *pubsub.Message) error { return nil })

	stopped := make(chan error, 1)
	go func() { stopped <- s.Run(context.Background()) }()

	// On SIGTERM, bounded by the pod's grace period:
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		fmt.Println("drain did not finish:", err)
	}
	fmt.Println(<-stopped)
	// Output: <nil>
}
