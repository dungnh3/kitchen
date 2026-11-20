package redis_test

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
	"github.com/dungnh3/kitchen/pubsub/reply"
	replyredis "github.com/dungnh3/kitchen/pubsub/reply/redis"
)

func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	broker, _ := replyredis.New(rdb)
	pub := memory.New()

	id := reply.NewID()
	w, _ := broker.Expect(ctx, id) // register before sending
	defer w.Close()
	_, _ = pub.Publish(ctx, "render-jobs", &pubsub.Message{
		Data: []byte("render #42"), Attributes: map[string]string{reply.IDAttribute: id},
	})
	result, err := w.Wait(ctx) // the worker calls broker.Reply(ctx, id, result)
	_, _ = result, err
}
