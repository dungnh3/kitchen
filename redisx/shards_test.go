package redisx_test

import (
	"context"
	"slices"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/redisx"
)

func TestGlobEscape(t *testing.T) {
	for in, want := range map[string]string{
		"plain:":     "plain:",
		"a*b?c":      `a\*b\?c`,
		"[x]-^\\":    `\[x\]\-\^\\`,
		"tenant:{1}": "tenant:{1}",
	} {
		if got := redisx.GlobEscape(in); got != want {
			t.Errorf("GlobEscape(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestForEachShardRing(t *testing.T) {
	ctx := context.Background()
	a, b := miniredis.RunT(t), miniredis.RunT(t)
	_ = a.Set("k:a", "1")
	_ = b.Set("k:b", "1")
	ring := redis.NewRing(&redis.RingOptions{
		Addrs:                 map[string]string{"a": a.Addr(), "b": b.Addr()},
		ContextTimeoutEnabled: true,
	})
	defer func() { _ = ring.Close() }()
	if redisx.IsCluster(ring) {
		t.Fatal("a Ring is not a Cluster")
	}
	if !redisx.ContextTimeoutEnabled(ring) {
		t.Fatal("ContextTimeoutEnabled = false")
	}
	var keys []string // no lock: calls are serialised
	err := redisx.ForEachShard(ctx, ring, func(ctx context.Context, shard redis.UniversalClient) error {
		ks, err := shard.Keys(ctx, "k:*").Result()
		keys = append(keys, ks...)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"k:a", "k:b"}) {
		t.Fatalf("keys = %v, want both shards", keys)
	}
}

func TestForEachShardSingle(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb, err := redisx.Open(ctx, redisx.Config{Addrs: []string{mr.Addr()}}, redisx.WithoutTracing())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rdb.Close() }()
	if redisx.IsCluster(rdb) || !redisx.ContextTimeoutEnabled(rdb) {
		t.Fatal("single node: IsCluster or ContextTimeoutEnabled wrong")
	}
	calls := 0
	if err := redisx.ForEachShard(ctx, rdb, func(context.Context, redis.UniversalClient) error {
		calls++
		return nil
	}); err != nil || calls != 1 {
		t.Fatalf("calls = %d, err = %v", calls, err)
	}
	cc := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{mr.Addr()}})
	defer func() { _ = cc.Close() }()
	if !redisx.IsCluster(cc) {
		t.Fatal("ClusterClient: IsCluster = false")
	}
}
