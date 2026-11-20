package lease_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/lease"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// memLocker is an in-memory Locker.
type memLocker struct {
	mu        sync.Mutex
	owner     string
	token     uint64
	fence     uint64
	expires   time.Time
	renewErr  error // injected Renew error
	releaseOK atomic.Int32
}

func (m *memLocker) Acquire(_ context.Context, name, owner string, ttl time.Duration) (lease.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.owner != "" && time.Now().Before(m.expires) {
		return lease.Lease{}, lease.ErrNotHeld
	}
	m.fence++
	m.owner, m.token, m.expires = owner, m.fence, time.Now().Add(ttl)
	return lease.Lease{Name: name, Owner: owner, Token: m.token, TTL: ttl}, nil
}

func (m *memLocker) Renew(_ context.Context, l lease.Lease) (lease.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.renewErr != nil {
		return lease.Lease{}, m.renewErr
	}
	if m.owner != l.Owner || m.token != l.Token || time.Now().After(m.expires) {
		return lease.Lease{}, lease.ErrNotHeld
	}
	m.expires = time.Now().Add(l.TTL)
	return l, nil
}

func (m *memLocker) Release(_ context.Context, l lease.Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.owner == l.Owner && m.token == l.Token {
		m.owner = ""
		m.releaseOK.Add(1)
	}
	return nil
}

func TestElectorSingleLeaderAndFailover(t *testing.T) {
	lk := &memLocker{}
	var leaders, maxLeaders atomic.Int32
	var mu sync.Mutex
	var tokens []uint64

	run := func(ctx context.Context, id string) chan error {
		e, err := lease.NewElector(lk, "jobs", lease.WithOwner(id), lease.WithTTL(90*time.Millisecond), lease.WithLogger(quiet))
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			done <- e.Run(ctx, func(ctx context.Context, l lease.Lease) error {
				n := leaders.Add(1)
				if n > maxLeaders.Load() {
					maxLeaders.Store(n)
				}
				mu.Lock()
				tokens = append(tokens, l.Token)
				mu.Unlock()
				<-ctx.Done()
				leaders.Add(-1)
				return nil
			})
		}()
		return done
	}

	ctxs := make([]context.CancelFunc, 3)
	dones := make([]chan error, 3)
	for i := range 3 {
		ctx, cancel := context.WithCancel(context.Background())
		ctxs[i], dones[i] = cancel, run(ctx, string(rune('a'+i)))
	}
	waitFor := func(cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatal("timeout")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor(func() bool { return leaders.Load() == 1 })
	lk.mu.Lock()
	first := lk.owner
	lk.mu.Unlock()
	ctxs[first[0]-'a']()
	waitFor(func() bool {
		lk.mu.Lock()
		defer lk.mu.Unlock()
		return leaders.Load() == 1 && lk.owner != first && lk.owner != ""
	})
	for i := range 3 {
		ctxs[i]()
		if err := <-dones[i]; err != nil {
			t.Fatal(err)
		}
	}
	if maxLeaders.Load() != 1 {
		t.Fatalf("max concurrent leaders = %d", maxLeaders.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(tokens) < 2 || tokens[1] <= tokens[0] {
		t.Fatalf("fencing tokens not increasing: %v", tokens)
	}
}

func TestElectorCancelsLeaderWhenRenewFails(t *testing.T) {
	for name, renewErr := range map[string]error{
		"lost":      lease.ErrNotHeld,
		"transient": errors.New("redis down"),
	} {
		t.Run(name, func(t *testing.T) {
			lk := &memLocker{}
			ttl := 90 * time.Millisecond
			e, err := lease.NewElector(lk, "jobs", lease.WithTTL(ttl), lease.WithRetryInterval(time.Hour), lease.WithLogger(quiet))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cause := make(chan error, 1)
			var held time.Duration
			go func() {
				_ = e.Run(ctx, func(lctx context.Context, l lease.Lease) error {
					start := time.Now()
					lk.mu.Lock()
					lk.renewErr = renewErr
					lk.mu.Unlock()
					<-lctx.Done()
					held = time.Since(start)
					cause <- context.Cause(lctx)
					return nil
				})
			}()
			select {
			case c := <-cause:
				if !errors.Is(c, lease.ErrLeaseLost) {
					t.Fatalf("cause = %v", c)
				}
				if held >= ttl {
					t.Fatalf("leader kept running %v, beyond ttl %v", held, ttl)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("leader not cancelled")
			}
			cancel()
		})
	}
}
