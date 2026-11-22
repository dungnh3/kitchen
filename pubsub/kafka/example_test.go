package kafka_test

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/kafka"
)

func Example() {
	ctx := context.Background()
	logger := log.Default() // main calls log.Setup once
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		logger.ErrorContext(ctx, "startup failed", log.Err(err))
		return
	}
	defer cl.Close()
	pub, _ := kafka.NewPublisher(cl)

	src, _ := kafka.NewSource("billing", []string{"orders"}, kafka.WithClientOptions(kgo.SeedBrokers("localhost:9092")))
	sub, _ := pubsub.NewSubscriber(src, func(_ context.Context, _ *pubsub.Message) error {
		return nil
	},
		// Kafka has no broker DLQ: after 5 deliveries, publish poison
		// messages to a dead-letter topic (nacked if that publish fails).
		pubsub.WithMaxDeliveryAttempts(5),
		pubsub.WithDeadLetter(pubsub.DeadLetterTo(pub, "orders.dlq")))
	_ = sub.Run(ctx)
}
