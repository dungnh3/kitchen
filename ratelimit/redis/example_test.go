package redis_test

import (
	"time"

	"github.com/dungnh3/kitchen/ratelimit"
	rlredis "github.com/dungnh3/kitchen/ratelimit/redis"
	goredis "github.com/redis/go-redis/v9"
)

func Example() {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:                  "localhost:6379",
		ContextTimeoutEnabled: true, // required
		ReadTimeout:           200 * time.Millisecond,
	})
	defer rdb.Close()

	store, err := rlredis.New(rdb)
	if err != nil {
		panic(err)
	}
	limiter, err := ratelimit.New(ratelimit.Config{
		Store:     store,
		Instances: 8, // divides limits when Redis is down and the local fallback is used
		Default: &ratelimit.Policy{
			Rule:         ratelimit.Rule{Rate: 20, Per: time.Second, Burst: 40},
			OnStoreError: ratelimit.FallbackLocal,
		},
	})
	if err != nil {
		panic(err)
	}
	_ = limiter // grpc.ChainUnaryInterceptor(..., limiter.UnaryServerInterceptor())
}
