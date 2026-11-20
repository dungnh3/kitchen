// Package lease provides named, expiring locks with fencing tokens and a
// leader [Elector] built on them.
//
// A lease is held by one owner until it expires or is released. Every
// successful Acquire returns a strictly increasing fencing token; downstream
// systems should reject writes carrying a token lower than one they have
// seen, which makes a paused, expired leader harmless.
//
// See [github.com/dungnh3/kitchen/lease/redis] for a Redis Locker. To split
// work into shards, take one lease per shard name.
package lease

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// ErrNotHeld is returned by Acquire when another owner holds the lease, and by
// Renew when the lease was lost.
var ErrNotHeld = errors.New("lease: not held")

// Lease is a held lease.
type Lease struct {
	Name  string
	Owner string
	// Token is the fencing token: strictly increasing per Name across
	// acquisitions.
	Token uint64
	TTL   time.Duration
}

// Locker manages leases. Implementations must make every operation atomic and
// owner-checked.
type Locker interface {
	// Acquire takes name for owner for ttl if it is free.
	Acquire(ctx context.Context, name, owner string, ttl time.Duration) (Lease, error)
	// Renew extends l by l.TTL if owner and token still match.
	Renew(ctx context.Context, l Lease) (Lease, error)
	// Release frees l if it is still held by l.Owner with l.Token. Releasing
	// a lost lease is not an error.
	Release(ctx context.Context, l Lease) error
}

// NewOwnerID returns "<hostname>-<random>", unique per call.
func NewOwnerID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "owner"
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	return host + "-" + hex.EncodeToString(b[:])
}

// ElectorOption configures an Elector.
type ElectorOption func(*Elector)

// WithOwner sets the owner ID (default NewOwnerID()).
func WithOwner(id string) ElectorOption { return func(e *Elector) { e.owner = id } }

// WithTTL sets the lease TTL (default 15s). The lease is renewed every TTL/3.
func WithTTL(d time.Duration) ElectorOption { return func(e *Elector) { e.ttl = d } }

// WithRetryInterval sets how often a follower tries to acquire (default TTL/3).
func WithRetryInterval(d time.Duration) ElectorOption { return func(e *Elector) { e.retry = d } }

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) ElectorOption { return func(e *Elector) { e.log = l } }

// Elector campaigns for a lease and runs a callback while it is the leader.
type Elector struct {
	locker Locker
	name   string
	owner  string
	ttl    time.Duration
	retry  time.Duration
	log    *slog.Logger
}

// NewElector returns an Elector for the lease name.
func NewElector(l Locker, name string, opts ...ElectorOption) (*Elector, error) {
	if l == nil || name == "" {
		return nil, errors.New("lease: nil locker or empty name")
	}
	e := &Elector{locker: l, name: name, ttl: 15 * time.Second}
	for _, o := range opts {
		o(e)
	}
	if e.ttl < 30*time.Millisecond {
		return nil, fmt.Errorf("lease: ttl %v too short", e.ttl)
	}
	if e.retry <= 0 {
		e.retry = e.ttl / 3
	}
	if e.owner == "" {
		e.owner = NewOwnerID()
	}
	if e.log == nil {
		e.log = slog.Default()
	}
	return e, nil
}

// Owner returns the owner ID used by the Elector.
func (e *Elector) Owner() string { return e.owner }

// ErrLeaseLost is the cause of the leader context's cancellation when the
// lease could not be renewed in time.
var ErrLeaseLost = errors.New("lease: lost")

// Run campaigns until ctx is cancelled and returns nil then.
//
// While this process holds the lease, lead runs with a context that is
// cancelled (cause ErrLeaseLost) before the lease can expire unless renewed,
// or when ctx is cancelled. lead must return promptly after its context is
// done; the lease is released only after lead returns, and lead never runs
// concurrently with itself. When lead returns, the lease is released and the
// campaign resumes. Use the lease's Token as a fencing token for writes.
func (e *Elector) Run(ctx context.Context, lead func(ctx context.Context, l Lease) error) error {
	for {
		start := time.Now()
		l, err := e.locker.Acquire(ctx, e.name, e.owner, e.ttl)
		switch {
		case err == nil:
			e.leadOnce(ctx, l, start, lead)
		case ctx.Err() != nil:
			return nil
		case !errors.Is(err, ErrNotHeld):
			e.log.Warn("lease: acquire failed", slog.String("lease", e.name), slog.Any("error", err))
		}
		t := time.NewTimer(e.retry)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}

// safety is the part of the TTL given up to clock drift and network delay.
func (e *Elector) safety() time.Duration { return e.ttl / 5 }

func (e *Elector) leadOnce(ctx context.Context, l Lease, validFrom time.Time, lead func(context.Context, Lease) error) {
	log := e.log.With(slog.String("lease", e.name), slog.Uint64("token", l.Token))
	log.Info("lease: acquired leadership")
	lctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	done := make(chan error, 1)
	held := l // l is reassigned by renewals below
	go func() {
		defer func() {
			if p := recover(); p != nil {
				done <- fmt.Errorf("lease: leader panic: %v", p)
			}
		}()
		done <- lead(lctx, held)
	}()

	expiry := time.NewTimer(time.Until(validFrom.Add(e.ttl - e.safety())))
	defer expiry.Stop()
	renew := time.NewTicker(e.ttl / 3)
	defer renew.Stop()

	var leadErr error
	finished := false
loop:
	for {
		select {
		case leadErr = <-done:
			finished = true
			break loop
		case <-lctx.Done():
			break loop
		case <-expiry.C:
			cancel(ErrLeaseLost)
			break loop
		case <-renew.C:
			start := time.Now()
			rctx, rcancel := context.WithTimeout(lctx, e.ttl/3)
			nl, err := e.locker.Renew(rctx, l)
			rcancel()
			switch {
			case err == nil:
				l = nl
				expiry.Reset(time.Until(start.Add(e.ttl - e.safety())))
			case errors.Is(err, ErrNotHeld):
				cancel(ErrLeaseLost)
				break loop
			default:
				log.Warn("lease: renew failed", slog.Any("error", err))
			}
		}
	}
	if !finished {
		leadErr = <-done
	}
	if leadErr != nil && !errors.Is(leadErr, context.Canceled) {
		log.Error("lease: leader returned an error", slog.Any("error", leadErr))
	}
	if cause := context.Cause(lctx); errors.Is(cause, ErrLeaseLost) {
		log.Warn("lease: leadership lost")
	}
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), e.ttl/3)
	defer rcancel()
	if err := e.locker.Release(rctx, l); err != nil {
		log.Warn("lease: release failed", slog.Any("error", err))
	}
	log.Info("lease: released leadership")
}
