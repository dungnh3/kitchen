package relay_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/sse"
	"github.com/dungnh3/kitchen/sse/redisbus"
	"github.com/dungnh3/kitchen/sse/relay"
)

// A held key whose backoff expires while the loop is blocked reading new
// entries must not publish the new entry before the held (pending) one.
func TestRelayHeldKeyNewEntryWaitsForPending(t *testing.T) {
	rdb, p := setup(t)
	var mu sync.Mutex
	failed := 0
	pub := &fakePub{fail: func(m *pubsub.Message) error {
		mu.Lock()
		defer mu.Unlock()
		if m.Key == "k" && failed == 0 {
			failed++
			return errors.New("unavailable")
		}
		return nil
	}}
	c := cfg("c1")
	c.Block = time.Second // the loop sits in XREADGROUP > while the hold expires
	c.RetryBackoff, c.MaxRetryBackoff = 20*time.Millisecond, 20*time.Millisecond
	r := start(t, rdb, c, pub)
	publish(t, p, "k", 1)
	eventually(t, "first publish failed", func() bool { mu.Lock(); defer mu.Unlock(); return failed == 1 })
	// Let the hold expire while the loop is blocked on new entries.
	time.Sleep(150 * time.Millisecond) //nolint:forbidigo // real Redis blocking read; synctest cannot drive it
	if _, err := p.Append(t.Context(), "k", msg("k-1")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "both published", func() bool { return len(pub.snapshot()) == 2 })
	eventually(t, "all acked", func() bool { return pendingCount(t, rdb, r) == 0 })
	var got []string
	for _, m := range pub.snapshot() {
		got = append(got, string(m.Data))
	}
	if !slices.Equal(got, []string{"k-0", "k-1"}) {
		t.Fatalf("order = %v, want [k-0 k-1]", got)
	}
}

func msg(data string) sse.Message { return sse.Message{Type: "run.step", Data: []byte(data)} }

// One key failing with more pending entries than one pass reads
// (maxPendingPages × Batch) must not stop the relay: other keys and shards
// keep flowing, and the loop does not spin on the pending list.
//
// miniredis ignores COUNT when it re-reads a pending list, so this needs a
// real Redis (KITCHEN_TEST_REDIS_ADDR).
func TestRelayLargePendingListDoesNotStarveOthers(t *testing.T) {
	rdb, p := setupReal(t)
	pub := &fakePub{fail: func(m *pubsub.Message) error {
		if m.Key == "bad" {
			return errors.New("unavailable")
		}
		return nil
	}}
	c := cfg("c1")
	c.Batch = 1 // one pass reads at most 100 pending entries
	r := start(t, rdb, c, pub)
	publish(t, p, "bad", 150)
	eventually(t, "bad entries pending", func() bool { return pendingCount(t, rdb, r) >= 100 })
	if _, err := p.Append(t.Context(), "good", msg("good-0")); err != nil {
		t.Fatal(err)
	}
	eventually(t, "good published", func() bool { return len(pub.snapshot()) == 1 })
}

// setupReal is setup on the Redis at KITCHEN_TEST_REDIS_ADDR, with the
// outbox keys deleted before and after the test.
func setupReal(t *testing.T) (*redis.Client, *redisbus.Producer) {
	t.Helper()
	addr := os.Getenv("KITCHEN_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("KITCHEN_TEST_REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	clean := func() {
		for b := range 4 {
			shard := fmt.Sprintf("%s:{b%d}", outbox, b)
			_ = rdb.Del(context.Background(), shard, relay.DLQKey(shard)).Err()
		}
	}
	clean()
	t.Cleanup(func() { clean(); _ = rdb.Close() })
	p, err := redisbus.NewProducer(rdb, redisbus.ProducerConfig{
		Keyspace: redisbus.Keyspace{Buckets: 4},
		Outbox:   &redisbus.OutboxConfig{Stream: outbox},
	})
	if err != nil {
		t.Fatal(err)
	}
	return rdb, p
}

// A deleted shard (and its consumer group) must not stop the relay forever.
func TestRelayRecreatesMissingGroup(t *testing.T) {
	rdb, p := setup(t)
	pub := &fakePub{}
	r := start(t, rdb, cfg("c1"), pub)
	publish(t, p, "runA", 1)
	eventually(t, "first published", func() bool { return len(pub.snapshot()) == 1 })
	for _, s := range r.Shards() {
		if err := rdb.Del(t.Context(), s).Err(); err != nil {
			t.Fatal(err)
		}
	}
	publish(t, p, "runA", 1)
	eventually(t, "published after the group was recreated", func() bool { return len(pub.snapshot()) == 2 })
}

// While a key is held longer than MinIdle, its pending entries are re-read
// often enough that they never look idle (and are not taken over by
// another replica).
func TestRelayHeldEntriesStayFresh(t *testing.T) {
	rdb, p := setup(t)
	pub := &fakePub{fail: func(*pubsub.Message) error { return errors.New("unavailable") }}
	c := cfg("c1")
	c.RetryBackoff, c.MaxRetryBackoff = 5*time.Second, 5*time.Second
	c.MinIdle, c.ReclaimInterval = 300*time.Millisecond, time.Hour
	r := start(t, rdb, c, pub)
	publish(t, p, "k", 1)
	eventually(t, "entry pending", func() bool { return pendingCount(t, rdb, r) == 1 })
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, s := range r.Shards() {
			ps, err := rdb.XPendingExt(t.Context(), &redis.XPendingExtArgs{Stream: s, Group: "g", Start: "-", End: "+", Count: 10}).Result()
			if err != nil {
				continue
			}
			for _, e := range ps {
				if e.Idle >= c.MinIdle {
					t.Fatalf("held entry idle %s >= MinIdle %s", e.Idle, c.MinIdle)
				}
			}
		}
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo // samples a real (miniredis) idle clock
	}
}
