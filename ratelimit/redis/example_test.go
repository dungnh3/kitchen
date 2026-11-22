package redis_test

import (
	"context"
	"time"

	rlredis "github.com/dungnh3/kitchen/ratelimit/redis"
	goredis "github.com/redis/go-redis/v9"
)

func Example() {
	// In a service, prefer redisx.Open (kitchen/redisx): it is traced,
	// metered and already has ContextTimeoutEnabled.
	rdb := goredis.NewClient(&goredis.Options{
		Addr:                  "localhost:6379",
		ContextTimeoutEnabled: true, // required
		ReadTimeout:           200 * time.Millisecond,
	})
	defer func() { _ = rdb.Close() }()

	store, err := rlredis.New(rdb)
	if err != nil {
		panic(err)
	}
	// Normally: ratelimit.New(ratelimit.Config{Store: ratelimit.RemoteStore(store), Instances: 8, ...})
	_, _, _, _, _ = store.GCRA(context.Background(), "rl:chat:user:u1", 20, 40, time.Second)
}
