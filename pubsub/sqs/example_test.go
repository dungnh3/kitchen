package sqs_test

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/sqs"
)

func Example() {
	ctx := context.Background()
	logger := log.Default() // main calls log.Setup once
	client := awssqs.New(awssqs.Options{Region: "ap-southeast-1", Credentials: aws.AnonymousCredentials{}})
	const queue = "https://sqs.ap-southeast-1.amazonaws.com/123456789012/orders.fifo"

	pub, _ := sqs.NewPublisher(client)
	if _, err := pub.Publish(ctx, queue, &pubsub.Message{
		Key: "customer-7", Data: []byte("hello"),
		Attributes: map[string]string{sqs.DedupIDAttribute: "order-1"},
	}); err != nil {
		logger.WarnContext(ctx, "publish failed", log.Err(err))
	}

	src, _ := sqs.NewSource(client, queue)
	sub, _ := pubsub.NewSubscriber(src, func(_ context.Context, _ *pubsub.Message) error { return nil })
	_ = sub.Run(ctx)
}
