package gcp_test

import (
	"context"

	gpubsub "cloud.google.com/go/pubsub/v2"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/gcp"
)

func Example() {
	ctx := context.Background()
	logger := log.Default() // main calls log.Setup once
	client, err := gpubsub.NewClient(ctx, "my-project")
	if err != nil {
		logger.ErrorContext(ctx, "startup failed", log.Err(err))
		return
	}
	defer func() { _ = client.Close() }()

	pub, _ := gcp.NewPublisher(client)
	defer func() { _ = pub.Close(ctx) }()
	if _, err := pub.Publish(ctx, "orders", &pubsub.Message{Key: "customer-7", Data: []byte(`{"id":1}`)}); err != nil {
		logger.WarnContext(ctx, "publish failed", log.Err(err)) // on ErrKeyPaused: handle, then pub.ResumeKey("orders", "customer-7")
	}

	// State the subscription's settings (message ordering on, dead-letter
	// policy of 5 attempts): roles/pubsub.subscriber cannot read them, and
	// the key fence needs the dead-letter limit.
	src, _ := gcp.NewSource(client, "orders-billing",
		gcp.WithSubscriptionConfig(true, 5),
		gcp.WithMaxOutstanding(1000, 100<<20))
	sub, _ := pubsub.NewSubscriber(src, func(_ context.Context, _ *pubsub.Message) error {
		return nil
	}, pubsub.WithConcurrency(64))
	_ = sub.Run(ctx) // blocks until ctx is cancelled, then drains
}
