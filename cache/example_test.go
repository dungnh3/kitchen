package cache_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/alicebob/miniredis/v2"
	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/cache"
	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/redisx"
)

type product struct {
	ID    string `json:"id"`
	Price int    `json:"price"`
}

// ErrProductNotFound is what a repository would return for a missing row.
var ErrProductNotFound = errs.New(codes.NotFound, "PRODUCT_NOT_FOUND", errs.Message("product not found"))

func ExampleNew() {
	ctx := context.Background()
	mr, _ := miniredis.Run() // in a service: your Redis address
	defer mr.Close()

	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	if err != nil {
		panic(err)
	}
	defer func() { _ = rdb.Close() }()

	products, err := cache.New(cache.Config[product]{
		Redis:  rdb,
		Prefix: "product:v1:",
		TTL:    10 * time.Minute,
		Local:  &cache.LocalConfig{MaxSize: 10_000, TTL: 30 * time.Second},
		Logger: log.Default(), // in a service: the *log.Logger from main
	})
	if err != nil {
		panic(err)
	}
	defer products.Close()

	p, err := products.GetOrLoad(ctx, "p1", func(_ context.Context, id string) (product, error) {
		return product{ID: id, Price: 100}, nil // e.g. repo.GetProduct(ctx, id)
	})
	fmt.Println(p.Price, err)

	_ = products.Set(ctx, "p2", product{ID: "p2", Price: 250})
	p, err = products.Get(ctx, "p2")
	fmt.Println(p.Price, err)

	_ = products.Delete(ctx, "p2")
	_, err = products.Get(ctx, "p2")
	fmt.Println(errors.Is(err, cache.ErrNotFound))
	// Output:
	// 100 <nil>
	// 250 <nil>
	// true
}

func ExampleCache_GetOrLoad_negative() {
	ctx := context.Background()
	c, _ := cache.New(cache.Config[product]{
		Local:       &cache.LocalConfig{MaxSize: 1000, TTL: time.Minute},
		NegativeTTL: 30 * time.Second, // remember "not found" for 30s
	})
	defer c.Close()

	calls := 0
	load := func(context.Context, string) (product, error) {
		calls++
		return product{}, ErrProductNotFound // any errs NotFound error
	}
	for range 3 {
		_, err := c.GetOrLoad(ctx, "missing", load)
		fmt.Println(errors.Is(err, cache.ErrNotFound))
	}
	fmt.Println("loader calls:", calls)
	// Output:
	// true
	// true
	// true
	// loader calls: 1
}

func ExampleCache_GetOrLoadMany() {
	ctx := context.Background()
	c, _ := cache.New(cache.Config[product]{Local: &cache.LocalConfig{MaxSize: 1000, TTL: time.Minute}})
	defer c.Close()

	_ = c.Set(ctx, "p1", product{ID: "p1", Price: 1})
	got, err := c.GetOrLoadMany(ctx, []string{"p1", "p2", "p3"}, func(_ context.Context, missing []string) (map[string]product, error) {
		fmt.Println("load", slices.Sorted(slices.Values(missing))) // one call, any order
		out := map[string]product{}
		for _, id := range missing {
			if id != "p3" { // p3 does not exist: omit it
				out[id] = product{ID: id, Price: 2}
			}
		}
		return out, nil
	})
	fmt.Println(slices.Sorted(maps.Keys(got)), err)
	// Output:
	// load [p2 p3]
	// [p1 p2] <nil>
}

func ExampleWithTTL() {
	ctx := context.Background()
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb, _ := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	defer func() { _ = rdb.Close() }()

	c, _ := cache.New(cache.Config[string]{
		Redis:  rdb,
		Prefix: "otp:",
		TTL:    time.Hour,
		Jitter: -1, // exact TTLs; the default ±10 % spreads expiries
	})
	defer c.Close()

	_ = c.Set(ctx, "u42", "123456", cache.WithTTL(5*time.Minute))
	fmt.Println(mr.TTL("otp:u42"))
	// Output: 5m0s
}

func ExampleConfig_clone() {
	type cart struct{ Items []string }
	c, _ := cache.New(cache.Config[*cart]{
		Local: &cache.LocalConfig{MaxSize: 1000, TTL: time.Minute},
		Clone: func(c *cart) *cart { return &cart{Items: slices.Clone(c.Items)} },
	})
	defer c.Close()

	ctx := context.Background()
	_ = c.Set(ctx, "u1", &cart{Items: []string{"apple"}})
	got, _ := c.Get(ctx, "u1")
	got.Items[0] = "changed" // mutates the copy only
	again, _ := c.Get(ctx, "u1")
	fmt.Println(again.Items[0])
	// Output: apple
}

func ExampleLocalConfig() {
	// L1 only: no Redis. RefreshAfter reloads hot keys in the background,
	// so readers never wait for a reload once a key is cached.
	c, err := cache.New(cache.Config[int]{
		Local:       &cache.LocalConfig{MaxSize: 1000, TTL: time.Minute, RefreshAfter: 45 * time.Second},
		LoadTimeout: 2 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	defer c.Close()
	n, err := c.GetOrLoad(context.Background(), "42", func(_ context.Context, k string) (int, error) {
		return strconv.Atoi(k)
	})
	fmt.Println(n, err)
	// Output: 42 <nil>
}

func ExamplePanicError() {
	c, _ := cache.New(cache.Config[int]{Local: &cache.LocalConfig{MaxSize: 10, TTL: time.Minute}})
	defer c.Close()
	_, err := c.GetOrLoad(context.Background(), "k", func(context.Context, string) (int, error) {
		panic("bug in loader")
	})
	var pe *cache.PanicError
	fmt.Println(errors.As(err, &pe), pe.Value)
	// Output: true bug in loader
}

func ExampleCache_DeleteByPrefix() {
	ctx := context.Background()
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb, _ := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	defer func() { _ = rdb.Close() }()

	c, _ := cache.New(cache.Config[int]{Redis: rdb, Prefix: "price:v1:", TTL: time.Hour})
	defer c.Close()
	for _, k := range []string{"shop1:a", "shop1:b", "shop2:a"} {
		_ = c.Set(ctx, k, 1)
	}
	n, err := c.DeleteByPrefix(ctx, "shop1:") // admin job, not a hot path
	fmt.Println(n, err)
	// Output: 2 <nil>
}

// rawCodec stores strings as raw bytes instead of JSON.
type rawCodec struct{}

func (rawCodec) Marshal(v string) ([]byte, error)       { return []byte(v), nil }
func (rawCodec) Unmarshal(data []byte, v *string) error { *v = string(data); return nil }

func ExampleCodec() {
	ctx := context.Background()
	mr, _ := miniredis.Run()
	defer mr.Close()
	rdb, _ := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}})
	defer func() { _ = rdb.Close() }()

	c, _ := cache.New(cache.Config[string]{Redis: rdb, Prefix: "html:", TTL: time.Hour, Codec: rawCodec{}})
	defer c.Close()
	_ = c.Set(ctx, "home", "<h1>hi</h1>")
	s, err := c.Get(ctx, "home")
	fmt.Println(s, err)
	// Output: <h1>hi</h1> <nil>
}
