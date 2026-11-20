package redis_test

import (
	"context"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/lease"
	leaseredis "github.com/dungnh3/kitchen/lease/redis"
)

func Example() {
	ctx := context.Background()
	rdb := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	defer rdb.Close()
	locker, _ := leaseredis.New(rdb)

	elector, _ := lease.NewElector(locker, "billing-scheduler")
	_ = elector.Run(ctx, func(ctx context.Context, l lease.Lease) error {
		// Leader only. Pass l.Token as a fencing token to protected writes;
		// return when ctx is done (lease lost or shutdown).
		<-ctx.Done()
		return nil
	})
}
