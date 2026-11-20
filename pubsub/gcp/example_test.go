package gcp_test

import (
	"context"
	"log"

	gpubsub "cloud.google.com/go/pubsub/v2"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/gcp"
)

func Example() {
	ctx := context.Background()
	client, err := gpubsub.NewClient(ctx, "my-project")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	pub, _ := gcp.NewPublisher(client)
	defer pub.Close(ctx)
	if _, err := pub.Publish(ctx, "orders", &pubsub.Message{Key: "customer-7", Data: []byte(`{"id":1}`)}); err != nil {
		log.Print(err) // on ErrKeyPaused: handle, then pub.ResumeKey("orders", "customer-7")
	}

	src, _ := gcp.NewSource(client, "orders-billing", gcp.WithMaxOutstanding(1000, 100<<20))
	sub, _ := pubsub.NewSubscriber(src, func(ctx context.Context, m *pubsub.Message) error {
		return nil
	}, pubsub.WithConcurrency(64))
	_ = sub.Run(ctx) // blocks until ctx is cancelled, then drains
}
