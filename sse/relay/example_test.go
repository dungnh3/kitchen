package relay_test

import (
	"context"
	"errors"
	"os"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
	"github.com/dungnh3/kitchen/redisx"
	"github.com/dungnh3/kitchen/sse/relay"
)

// A relay drains the outbox shards that redisbus.Producer writes and
// publishes every entry to a pubsub topic.
func ExampleNew() {
	ctx := context.Background()
	logger := log.Default()
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{"localhost:6379"}})
	if err != nil {
		logger.ErrorContext(ctx, "redis", log.Err(err))
		return
	}
	defer func() { _ = rdb.Close() }()

	var pub pubsub.Publisher = memory.New() // pubsub/gcp with ordering in production
	hostname, _ := os.Hostname()
	r, err := relay.New(rdb, relay.Config{
		Stream:   "outbox:orch", // = redisbus.OutboxConfig.Stream
		Topic:    "agent-events",
		Group:    "relay",
		Consumer: hostname, // unique per replica
		Route:    relay.EnvelopeRoute("orchestrator"),
		Classify: func(err error) relay.Class {
			if errors.Is(err, pubsub.ErrPermanent) {
				return relay.Poison // dead-lettered after PoisonAttempts
			}
			return relay.Transient // retried with backoff
		},
		Logger: logger,
	}, pub)
	if err != nil {
		logger.ErrorContext(ctx, "relay", log.Err(err))
		return
	}
	_ = r.Run(ctx) // returns when ctx is done
}
