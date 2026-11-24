package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/cache"
)

func benchCache(b *testing.B) *cache.Cache[user] {
	b.Helper()
	c, err := cache.New(cache.Config[user]{Name: "bench", Local: &cache.LocalConfig{MaxSize: 1000, TTL: time.Hour}})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func loadUser(_ context.Context, id string) (user, error) { return user{ID: id}, nil }

func BenchmarkGetHitL1(b *testing.B) {
	c := benchCache(b)
	ctx := context.Background()
	_ = c.Set(ctx, "k", user{ID: "k"})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = c.Get(ctx, "k")
		}
	})
}

func BenchmarkGetMissL1(b *testing.B) {
	c := benchCache(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = c.Get(ctx, "k")
		}
	})
}

func BenchmarkGetOrLoadHitL1(b *testing.B) {
	c := benchCache(b)
	ctx := context.Background()
	_, _ = c.GetOrLoad(ctx, "k", loadUser)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = c.GetOrLoad(ctx, "k", loadUser)
		}
	})
}

// A miss: L1 lookup, one flight, loader, L1 fill; then Delete.
func BenchmarkGetOrLoadMissL1(b *testing.B) {
	c := benchCache(b)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = c.GetOrLoad(ctx, "k", loadUser)
		_ = c.Delete(ctx, "k")
	}
}
