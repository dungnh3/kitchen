package relay

import (
	"context"
	_ "embed"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

//go:embed replay.lua
var replayLua string

var replayScript = redis.NewScript(replayLua)

// Replay moves up to limit dead letters (per shard; limit <= 0 means 1000)
// back to their outbox shards, where the relay publishes them again. It
// returns how many were moved.
func (r *Relay) Replay(ctx context.Context, limit int64) (int, error) {
	if limit <= 0 {
		limit = 1000
	}
	moved := 0
	for _, shard := range r.Shards() {
		dlq := DLQKey(shard)
		msgs, err := r.rdb.XRangeN(ctx, dlq, "-", "+", limit).Result()
		if err != nil {
			return moved, fmt.Errorf("relay: replay read %s: %w", dlq, err)
		}
		for _, m := range msgs {
			err := replayScript.Run(ctx, r.rdb, []string{shard, dlq}, m.ID).Err()
			if err != nil && !errors.Is(err, redis.Nil) {
				return moved, fmt.Errorf("relay: replay %s: %w", m.ID, err)
			}
			if err == nil {
				moved++
			}
		}
	}
	return moved, nil
}
