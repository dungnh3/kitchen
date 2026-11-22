package redisx_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/cache"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/redisx"
)

// Redis Cluster: several seed addresses (or Cluster: true with one), TLS
// and ACL credentials. Compile-only: it needs a cluster.
func ExampleOpen_cluster() {
	ctx := context.Background()
	rdb, err := redisx.Open(ctx, redisx.Config{
		Addrs:    []string{"redis-0:6379", "redis-1:6379", "redis-2:6379"},
		Username: "orders",
		Password: os.Getenv("REDIS_PASSWORD"),
		TLS:      &tls.Config{MinVersion: tls.VersionTLS12},
		PoolSize: 50,
	})
	if err != nil {
		log.Default().Error("redis", log.Err(err))
		return
	}
	defer func() { _ = rdb.Close() }()
}

// Sentinel: MasterName set, Addrs are the sentinels. Compile-only.
func ExampleOpen_sentinel() {
	ctx := context.Background()
	rdb, err := redisx.Open(ctx, redisx.Config{
		MasterName:       "mymaster",
		Addrs:            []string{"sentinel-0:26379", "sentinel-1:26379", "sentinel-2:26379"},
		SentinelPassword: os.Getenv("SENTINEL_PASSWORD"),
		Password:         os.Getenv("REDIS_PASSWORD"),
		DB:               2,
	})
	if err != nil {
		log.Default().Error("redis", log.Err(err))
		return
	}
	defer func() { _ = rdb.Close() }()
}

func ExampleConfig_UniversalOptions() {
	uo, err := redisx.Config{Addrs: []string{"redis-0:6379"}, Cluster: true}.UniversalOptions()
	if err != nil {
		panic(err)
	}
	fmt.Println(uo.IsClusterMode, uo.ContextTimeoutEnabled)
	// Output: true true
}

func ExampleWithoutTracing() {
	mr, _ := miniredis.Run()
	defer mr.Close()

	// Metrics stay on; per-command spans are dropped (hot paths, noisy traces).
	rdb, err := redisx.Open(context.Background(), redisx.Config{Addrs: []string{mr.Addr()}}, redisx.WithoutTracing())
	if err != nil {
		panic(err)
	}
	defer func() { _ = rdb.Close() }()
	fmt.Println(redisx.ContextTimeoutEnabled(rdb), redisx.IsCluster(rdb))
	// Output: true false
}

func ExampleForEachShard() {
	mr, _ := miniredis.Run()
	defer mr.Close()
	ctx := context.Background()
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	if err != nil {
		panic(err)
	}
	defer func() { _ = rdb.Close() }()
	_ = rdb.Set(ctx, "tenant[1]:a", 1, 0).Err()
	_ = rdb.Set(ctx, "tenant[1]:b", 1, 0).Err()
	_ = rdb.Set(ctx, "tenant12:a", 1, 0).Err()

	// SCAN runs on every master of a Cluster (and once on a single node).
	match := redisx.GlobEscape("tenant[1]:") + "*" // "[1]" is literal, not a class
	n := 0
	err = redisx.ForEachShard(ctx, rdb, func(ctx context.Context, shard redis.UniversalClient) error {
		it := shard.Scan(ctx, 0, match, 100).Iterator()
		for it.Next(ctx) {
			n++ // calls are serialised: no lock needed
		}
		return it.Err()
	})
	fmt.Println(n, err)
	// Output: 2 <nil>
}

// cache takes the client from Open as its L2.
func Example_cache() {
	mr, _ := miniredis.Run()
	defer mr.Close()
	ctx := context.Background()
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	if err != nil {
		panic(err)
	}
	defer func() { _ = rdb.Close() }()

	type user struct{ Name string }
	users, err := cache.New(cache.Config[user]{
		Redis:  rdb,
		Prefix: "user:v1:",
		TTL:    10 * time.Minute,
	})
	if err != nil {
		panic(err)
	}
	defer users.Close()
	u, err := users.GetOrLoad(ctx, "42", func(context.Context, string) (user, error) {
		return user{Name: "Ann"}, nil
	})
	fmt.Println(u.Name, err)
	// Output: Ann <nil>
}
