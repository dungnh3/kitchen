package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/lease"
	leaseredis "github.com/dungnh3/kitchen/lease/redis"
	"github.com/dungnh3/kitchen/log"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func newLocker(t *testing.T) (*leaseredis.Locker, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	l, err := leaseredis.New(rdb)
	if err != nil {
		t.Fatal(err)
	}
	return l, mr
}

func TestLockerLifecycle(t *testing.T) {
	l, mr := newLocker(t)
	ctx := context.Background()

	a, err := l.Acquire(ctx, "shard-1", "a", 500*time.Millisecond)
	if err != nil || a.Token != 1 {
		t.Fatalf("acquire a = %+v, %v", a, err)
	}
	if _, err := l.Acquire(ctx, "shard-1", "b", time.Second); !errors.Is(err, lease.ErrNotHeld) {
		t.Fatalf("second acquire = %v", err)
	}
	if _, err := l.Renew(ctx, a); err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL("lease:{shard-1}"); ttl <= 0 || ttl > 500*time.Millisecond {
		t.Fatalf("ttl after renew = %v (want ms precision)", ttl)
	}

	mr.FastForward(time.Second) // a expires
	b, err := l.Acquire(ctx, "shard-1", "b", time.Second)
	if err != nil || b.Token != 2 {
		t.Fatalf("acquire b = %+v, %v", b, err)
	}
	// The expired owner can neither renew nor release b's lease.
	if _, err := l.Renew(ctx, a); !errors.Is(err, lease.ErrNotHeld) {
		t.Fatalf("stale renew = %v", err)
	}
	if err := l.Release(ctx, a); err != nil {
		t.Fatal(err)
	}
	if !mr.Exists("lease:{shard-1}") {
		t.Fatal("stale release deleted the new owner's lease")
	}
	if err := l.Release(ctx, b); err != nil {
		t.Fatal(err)
	}
	if mr.Exists("lease:{shard-1}") {
		t.Fatal("release did not delete")
	}
	if _, err := l.Acquire(ctx, "bad{name}", "a", time.Second); err == nil {
		t.Fatal("invalid name accepted")
	}
}

func TestElectorWithRedis(t *testing.T) {
	l, _ := newLocker(t)
	e, err := lease.NewElector(l, "jobs", lease.WithTTL(300*time.Millisecond),
		lease.WithLogger(log.Discard()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	led := make(chan uint64, 1)
	done := make(chan error)
	go func() {
		done <- e.Run(ctx, func(ctx context.Context, ls lease.Lease) error {
			led <- ls.Token
			<-ctx.Done()
			return nil
		})
	}()
	select {
	case tok := <-led:
		if tok != 1 {
			t.Fatalf("token = %d", tok)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("never elected")
	}
	time.Sleep(400 * time.Millisecond) // survives renewals beyond one TTL
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
