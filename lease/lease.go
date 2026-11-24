// Package lease provides named, expiring locks with fencing tokens and a
// leader [Elector] built on them.
//
// A Locker grants a lease to one owner at a time until it expires or is
// released. A Locker on replicated storage can break this after a failover
// (see lease/redis). Every successful Acquire returns a strictly increasing
// fencing token; downstream systems should reject writes carrying a token
// lower than one they have seen, which makes a paused, expired leader
// harmless.
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
	"os"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
)

// ErrNotHeld is returned by Acquire (and Do) when another owner holds the
// lease, and by Renew when the lease was lost. It is an errs error with code
// Aborted, so a server answers 409 "try again".
var ErrNotHeld = errs.New(codes.Aborted, "LEASE_NOT_HELD", errs.Message("the resource is locked by another operation"))

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

// WithLogger sets the logger (default log.Default()).
func WithLogger(l *log.Logger) ElectorOption { return func(e *Elector) { e.log = l } }

// Elector campaigns for a lease and runs a callback while it is the leader.
type Elector struct {
	locker Locker
	name   string
	owner  string
	ttl    time.Duration
	retry  time.Duration
	log    *log.Logger
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
	e.log = log.OrDefault(e.log)
	return e, nil
}

// Owner returns the owner ID used by the Elector.
func (e *Elector) Owner() string { return e.owner }

// ErrLeaseLost is the cause of the leader's (or Do's) context cancellation
// when the lease could not be renewed in time. It is an errs error with code
// Aborted.
var ErrLeaseLost = errs.New(codes.Aborted, "LEASE_LOST", errs.Message("the lock was lost before the operation finished"))

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
			e.log.WarnContext(ctx, "lease: acquire failed", log.String("lease", e.name), log.Err(err))
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

func (e *Elector) leadOnce(ctx context.Context, l Lease, validFrom time.Time, lead func(context.Context, Lease) error) {
	lg := e.log.With(log.String("lease", e.name), log.Uint64("token", l.Token))
	lg.InfoContext(ctx, "lease: acquired leadership")
	lost, leadErr := hold(ctx, e.locker, l, validFrom, lg, lead)
	if leadErr != nil && !errors.Is(leadErr, context.Canceled) {
		lg.ErrorContext(ctx, "lease: leader returned an error", log.Err(leadErr))
	}
	if lost {
		lg.WarnContext(ctx, "lease: leadership lost")
	}
	lg.InfoContext(ctx, "lease: released leadership")
}

// hold runs fn while l is held: it renews l every TTL/3, cancels fn's context
// (cause ErrLeaseLost) when the lease is lost or would expire before a
// renewal (TTL/5 safety margin), and releases l after fn returns. A panic in
// fn is returned as an error. lost reports whether the lease was lost.
func hold(ctx context.Context, locker Locker, l Lease, validFrom time.Time, lg *log.Logger, fn func(context.Context, Lease) error) (lost bool, fnErr error) {
	safety := l.TTL / 5
	lctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	done := make(chan error, 1)
	held := l // l is reassigned by renewals below
	go func() {
		defer func() {
			if p := recover(); p != nil {
				done <- fmt.Errorf("lease: %s: panic: %v", held.Name, p)
			}
		}()
		done <- fn(lctx, held)
	}()

	expiry := time.NewTimer(time.Until(validFrom.Add(l.TTL - safety)))
	defer expiry.Stop()
	renew := time.NewTicker(l.TTL / 3)
	defer renew.Stop()

	finished := false
loop:
	for {
		select {
		case fnErr = <-done:
			finished = true
			break loop
		case <-lctx.Done():
			break loop
		case <-expiry.C:
			cancel(ErrLeaseLost)
			break loop
		case <-renew.C:
			start := time.Now()
			rctx, rcancel := context.WithTimeout(lctx, l.TTL/3)
			nl, err := locker.Renew(rctx, l)
			rcancel()
			switch {
			case err == nil:
				l = nl
				expiry.Reset(time.Until(start.Add(l.TTL - safety)))
			case errors.Is(err, ErrNotHeld):
				cancel(ErrLeaseLost)
				break loop
			default:
				lg.WarnContext(ctx, "lease: renew failed", log.Err(err))
			}
		}
	}
	if !finished {
		fnErr = <-done
	}
	lost = errors.Is(context.Cause(lctx), ErrLeaseLost)
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), l.TTL/3)
	defer rcancel()
	if err := locker.Release(rctx, l); err != nil {
		lg.WarnContext(ctx, "lease: release failed", log.Err(err))
	}
	return lost, fnErr
}

// DoOption configures Do.
type DoOption func(*doOptions)

type doOptions struct {
	owner string
	log   *log.Logger
}

// DoOwner sets the owner ID (default NewOwnerID()).
func DoOwner(id string) DoOption { return func(o *doOptions) { o.owner = id } }

// DoLogger sets the logger for renewal and release failures (default
// log.Default()).
func DoLogger(l *log.Logger) DoOption { return func(o *doOptions) { o.log = l } }

// Do runs fn while holding the lease name: a mutex across processes. It
// returns ErrNotHeld at once when another owner holds the lease (it does not
// wait). While fn runs the lease is renewed every ttl/3; if it is lost, fn's
// context is cancelled with cause ErrLeaseLost and Do returns an error
// wrapping ErrLeaseLost (joined with fn's error). The lease is released when
// fn returns. Pass l.Token to the resource you protect as a fencing token.
//
// Unlike "SETNX key, do work, DEL key", Do renews the lease, checks the
// owner on release and gives fn a fencing token.
func Do(ctx context.Context, locker Locker, name string, ttl time.Duration, fn func(ctx context.Context, l Lease) error, opts ...DoOption) error {
	o := doOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	if o.owner == "" {
		o.owner = NewOwnerID()
	}
	o.log = log.OrDefault(o.log)
	if locker == nil || name == "" || ttl < 30*time.Millisecond {
		return errors.New("lease: Do needs a locker, a name and a ttl of at least 30ms")
	}
	start := time.Now()
	l, err := locker.Acquire(ctx, name, o.owner, ttl)
	if err != nil {
		return err
	}
	lost, fnErr := hold(ctx, locker, l, start, o.log.With(log.String("lease", name), log.Uint64("token", l.Token)), fn)
	if lost {
		return errors.Join(fnErr, errs.Wrap(fmt.Errorf("lease: %s lost while running", name), ErrLeaseLost))
	}
	return fnErr
}
