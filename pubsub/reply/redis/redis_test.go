package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/goleak"

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
	defer w.Close()
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
	defer w.Close()
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
	defer w.Close()
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
