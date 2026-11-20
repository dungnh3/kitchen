package kafka_test

import (
	"context"
	"log"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/kafka"
)

func Example() {
	ctx := context.Background()
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		log.Fatal(err)
	}
	defer cl.Close()
	pub, _ := kafka.NewPublisher(cl)

	src, _ := kafka.NewSource("billing", []string{"orders"}, kafka.WithClientOptions(kgo.SeedBrokers("localhost:9092")))
	sub, _ := pubsub.NewSubscriber(src, func(ctx context.Context, m *pubsub.Message) error {
		return nil
	},
		// Kafka has no broker DLQ: route poison messages to a topic.
		pubsub.WithMaxDeliveryAttempts(5),
		pubsub.WithDeadLetter(func(ctx context.Context, m *pubsub.Message, cause error) error {
			_, err := pub.Publish(ctx, m.Topic+".dlq", &pubsub.Message{Key: m.Key, Data: m.Data, Attributes: m.Attributes})
			return err
		}))
	_ = sub.Run(ctx)
}
