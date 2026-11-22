package redis_test

import (
	"context"
	"time"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
	"github.com/dungnh3/kitchen/pubsub/reply"
	replyredis "github.com/dungnh3/kitchen/pubsub/reply/redis"
	"github.com/dungnh3/kitchen/redisx"
)

func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// redisx.Open: tracing, metrics, context deadlines honored, ping on open.
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{"localhost:6379"}})
	if err != nil {
		return
	}
	defer func() { _ = rdb.Close() }()
	broker, _ := replyredis.New(rdb)
	pub := memory.New()

	id := reply.NewID()
	w, _ := broker.Expect(ctx, id) // register before sending
	defer func() { _ = w.Close() }()
	_, _ = pub.Publish(ctx, "render-jobs", &pubsub.Message{
		Data: []byte("render #42"), Attributes: map[string]string{reply.IDAttribute: id},
	})
	// The worker, after rendering: broker.Reply(ctx, m.Attributes[reply.IDAttribute], result)
	go func() { _ = broker.Reply(ctx, id, []byte("render #42 done")) }()

	result, err := w.Wait(ctx) // blocks until the reply, ctx's deadline or Close
	_, _ = result, err
}
