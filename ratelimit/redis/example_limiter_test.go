package redis_test

import (
	"context"
	"fmt"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dungnh3/kitchen/ratelimit"
	rlredis "github.com/dungnh3/kitchen/ratelimit/redis"
	"github.com/dungnh3/kitchen/redisx"
)

func Example_limiter() {
	mr, _ := miniredis.Run() // a real deployment points redisx at Redis
	defer mr.Close()
	ctx := context.Background()

	// redisx.Open: traced, metered, ContextTimeoutEnabled already set.
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	if err != nil {
		panic(err)
	}
	defer func() { _ = rdb.Close() }()
	store, err := rlredis.New(rdb)
	if err != nil {
		panic(err)
	}
	l, err := ratelimit.New(ratelimit.Config{
		Store:     ratelimit.RemoteStore(store), // buckets shared by every instance
		Instances: 4,                            // divides rules while Redis is down
		Policies: map[string]ratelimit.Policy{
			"send-otp": {Rule: ratelimit.Rule{Rate: 1, Per: time.Minute}},
		},
	})
	if err != nil {
		panic(err)
	}
	for range 2 {
		d, _ := l.Allow(ctx, "send-otp", "phone:+84900000000")
		fmt.Println(d.Allowed, d.Remaining)
	}
	// Output:
	// true 0
	// false 0
}
