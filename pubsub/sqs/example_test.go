package sqs_test

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/sqs"
)

func Example() {
	ctx := context.Background()
	client := awssqs.New(awssqs.Options{Region: "ap-southeast-1", Credentials: aws.AnonymousCredentials{}})
	const queue = "https://sqs.ap-southeast-1.amazonaws.com/123456789012/orders.fifo"

	pub, _ := sqs.NewPublisher(client)
	if _, err := pub.Publish(ctx, queue, &pubsub.Message{Key: "customer-7", Data: []byte("hello"),
		Attributes: map[string]string{sqs.DedupIDAttribute: "order-1"}}); err != nil {
		log.Print(err)
	}

	src, _ := sqs.NewSource(client, queue)
	sub, _ := pubsub.NewSubscriber(src, func(ctx context.Context, m *pubsub.Message) error { return nil })
	_ = sub.Run(ctx)
}
