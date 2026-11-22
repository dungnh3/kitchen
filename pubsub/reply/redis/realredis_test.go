package redis_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dungnh3/kitchen/pubsub/reply"
	replyredis "github.com/dungnh3/kitchen/pubsub/reply/redis"
)

// redisLogTap captures go-redis' internal log output (a process-wide logger)
// for the duration of a test.
type redisLogTap struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *redisLogTap) Printf(_ context.Context, format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.buf, format+"\n", v...)
}

func (l *redisLogTap) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// realRedis connects to KITCHEN_TEST_REDIS_ADDR with the options redisx.Open
// uses (ContextTimeoutEnabled), so socket deadlines follow ctx deadlines.
func realRedis(t *testing.T) *goredis.Client {
	t.Helper()
	addr := os.Getenv("KITCHEN_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("KITCHEN_TEST_REDIS_ADDR not set")
	}
	rdb := goredis.NewClient(&goredis.Options{Addr: addr, ContextTimeoutEnabled: true, MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping %s: %v", addr, err)
	}
	return rdb
}

// A timed-out Wait must return context.DeadlineExceeded close to the
// deadline, keep its pooled connection (no socket read timeout) and print
// nothing through go-redis' logger.
func TestWaitDeadlineAgainstRealRedis(t *testing.T) {
	rdb := realRedis(t)
	tap := &redisLogTap{}
	goredis.SetLogger(tap)
	t.Cleanup(func() { goredis.SetLogger(&discardRedisLog{}) })

	b, err := replyredis.New(rdb)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []time.Duration{300 * time.Millisecond, 1500 * time.Millisecond, 2500 * time.Millisecond} {
		t.Run(d.String(), func(t *testing.T) {
			ctx := context.Background()
			w, err := b.Expect(ctx, reply.NewID())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = w.Close() }()
			before := rdb.PoolStats()
			wctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()
			start := time.Now()
			_, err = w.Wait(wctx)
			elapsed := time.Since(start)
			after := rdb.PoolStats()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Wait = %v (%T), want context.DeadlineExceeded", err, err)
			}
			if elapsed < d || elapsed > d+300*time.Millisecond {
				t.Errorf("Wait returned after %s, want about %s", elapsed, d)
			}
			if after.Misses != before.Misses || after.StaleConns != before.StaleConns {
				t.Errorf("Wait destroyed its connection: misses %d->%d, stale %d->%d",
					before.Misses, after.Misses, before.StaleConns, after.StaleConns)
			}
		})
	}
	if out := tap.String(); strings.TrimSpace(out) != "" {
		t.Errorf("go-redis logged during Wait:\n%s", out)
	}
}

// A reply that arrives while Wait blocks is delivered promptly.
func TestWaitReceivesReplyAgainstRealRedis(t *testing.T) {
	rdb := realRedis(t)
	b, err := replyredis.New(rdb)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id := reply.NewID()
	w, _ := b.Expect(ctx, id)
	defer func() { _ = w.Close() }()
	var wg sync.WaitGroup
	wg.Go(func() {
		time.Sleep(200 * time.Millisecond)
		_ = b.Reply(ctx, id, []byte("ok"))
	})
	got, err := w.Wait(ctx)
	wg.Wait()
	if err != nil || string(got) != "ok" {
		t.Fatalf("Wait = %q, %v", got, err)
	}
}

type discardRedisLog struct{}

func (discardRedisLog) Printf(context.Context, string, ...any) {}
