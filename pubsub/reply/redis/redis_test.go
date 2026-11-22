package redis_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/pubsub/reply"
	replyredis "github.com/dungnh3/kitchen/pubsub/reply/redis"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func newBroker(t *testing.T, opts ...replyredis.Option) (*replyredis.Broker, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	b, err := replyredis.New(rdb, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return b, mr
}

func TestReplyBeforeWaitIsNotLost(t *testing.T) {
	b, _ := newBroker(t)
	ctx := context.Background()
	id := reply.NewID()
	w, err := b.Expect(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if err := b.Reply(ctx, id, []byte("done")); err != nil {
		t.Fatal(err)
	}
	got, err := w.Wait(ctx)
	if err != nil || string(got) != "done" {
		t.Fatalf("Wait = %q, %v", got, err)
	}
}

func TestWaitBlocksUntilReply(t *testing.T) {
	b, _ := newBroker(t)
	ctx := context.Background()
	id := reply.NewID()
	w, _ := b.Expect(ctx, id)
	defer func() { _ = w.Close() }()
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = b.Reply(ctx, id, []byte("late"))
	}()
	got, err := w.Wait(ctx)
	if err != nil || string(got) != "late" {
		t.Fatalf("Wait = %q, %v", got, err)
	}
}

func TestWaitHonoursContext(t *testing.T) {
	b, _ := newBroker(t)
	w, _ := b.Expect(context.Background(), reply.NewID())
	defer func() { _ = w.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := w.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait = %v", err)
	}
}

func TestReplyExpiresAndValidates(t *testing.T) {
	b, mr := newBroker(t, replyredis.WithTTL(time.Second), replyredis.WithMaxPayload(4))
	ctx := context.Background()
	id := reply.NewID()
	if err := b.Reply(ctx, id, []byte("ok")); err != nil {
		t.Fatal(err)
	}
	mr.FastForward(2 * time.Second)
	if mr.Exists("reply:" + id) {
		t.Fatal("reply did not expire")
	}
	if err := b.Reply(ctx, id, []byte("too big")); err == nil {
		t.Fatal("oversized payload accepted")
	}
	if err := b.Reply(ctx, "bad id*", nil); !errors.Is(err, reply.ErrInvalidID) {
		t.Fatalf("invalid id: %v", err)
	}
}

// Close reports Redis failures like Reply and Wait: wrapped as
// DEPENDENCY_UNAVAILABLE, with the package prefix.
func TestCloseWrapsRedisError(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr(), MaxRetries: -1})
	b, err := replyredis.New(rdb)
	if err != nil {
		t.Fatal(err)
	}
	w, _ := b.Expect(context.Background(), reply.NewID())
	_ = rdb.Close()
	err = w.Close()
	if !errors.Is(err, errs.ErrDependencyUnavailable) || !strings.Contains(err.Error(), "reply/redis: close: ") {
		t.Fatalf("Close = %v (%T), want reply/redis error wrapping DEPENDENCY_UNAVAILABLE", err, err)
	}
}
