package lease_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/lease"
	"github.com/dungnh3/kitchen/log"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var quiet = log.Discard()

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
				_ = e.Run(ctx, func(lctx context.Context, _ lease.Lease) error {
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

func TestDo(t *testing.T) {
	ctx := context.Background()
	lk := &memLocker{}
	ttl := 90 * time.Millisecond

	var token uint64
	err := lease.Do(ctx, lk, "job", ttl, func(ctx context.Context, l lease.Lease) error {
		token = l.Token
		// Held for several TTLs: renewals keep it.
		select {
		case <-time.After(3 * ttl):
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		// Another owner cannot take it meanwhile.
		if err := lease.Do(ctx, lk, "job", ttl, func(context.Context, lease.Lease) error { return nil }); !errors.Is(err, lease.ErrNotHeld) || errs.Code(err) != codes.Aborted {
			t.Errorf("concurrent Do = %v", err)
		}
		return nil
	}, lease.DoLogger(quiet))
	if err != nil || token == 0 || lk.releaseOK.Load() != 1 {
		t.Fatalf("Do = %v token=%d released=%d", err, token, lk.releaseOK.Load())
	}

	boom := errors.New("boom")
	if err := lease.Do(ctx, lk, "job", ttl, func(context.Context, lease.Lease) error { return boom }, lease.DoLogger(quiet)); !errors.Is(err, boom) {
		t.Fatalf("fn error = %v", err)
	}
	if err := lease.Do(ctx, lk, "job", ttl, func(context.Context, lease.Lease) error { panic("p") }, lease.DoLogger(quiet)); err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("panic = %v", err)
	}

	err = lease.Do(ctx, lk, "job", ttl, func(ctx context.Context, _ lease.Lease) error {
		lk.mu.Lock()
		lk.renewErr = lease.ErrNotHeld
		lk.mu.Unlock()
		<-ctx.Done()
		return ctx.Err()
	}, lease.DoLogger(quiet))
	if !errors.Is(err, lease.ErrLeaseLost) {
		t.Fatalf("lost lease = %v", err)
	}
}
