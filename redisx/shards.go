package redisx

import (
	"context"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// IsCluster reports whether c is a Redis Cluster client, where a multi-key
// command must keep all its keys in one hash slot (use a {hash tag}).
func IsCluster(c redis.UniversalClient) bool {
	_, ok := c.(*redis.ClusterClient)
	return ok
}

// ForEachShard calls fn once for every node that holds data: each master of
// a Cluster client, each shard of a Ring, or c itself otherwise. Use it for
// keyspace-wide commands (SCAN, FLUSH, INFO) that a Cluster or Ring client
// would otherwise send to a single node. Calls are serialised, so fn may
// update shared state without locking. It stops at the first error.
func ForEachShard(ctx context.Context, c redis.UniversalClient, fn func(ctx context.Context, shard redis.UniversalClient) error) error {
	var mu sync.Mutex
	serial := func(ctx context.Context, shard *redis.Client) error {
		mu.Lock()
		defer mu.Unlock()
		return fn(ctx, shard)
	}
	switch cc := c.(type) {
	case *redis.ClusterClient:
		return cc.ForEachMaster(ctx, serial)
	case *redis.Ring:
		return cc.ForEachShard(ctx, serial)
	}
	return fn(ctx, c)
}

// ContextTimeoutEnabled reports whether c honours context deadlines.
// Without it go-redis ignores ctx deadlines and uses its own timeouts.
// Clients from [Open] always have it set.
func ContextTimeoutEnabled(c redis.UniversalClient) bool {
	switch cc := c.(type) {
	case *redis.Client:
		return cc.Options().ContextTimeoutEnabled
	case *redis.ClusterClient:
		return cc.Options().ContextTimeoutEnabled
	case *redis.Ring:
		return cc.Options().ContextTimeoutEnabled
	}
	return false
}

// GlobEscape escapes the glob metacharacters of s for a SCAN MATCH or KEYS
// pattern, so GlobEscape(prefix)+"*" matches keys starting with prefix.
func GlobEscape(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '*', '?', '[', ']', '\\', '^', '-':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}
