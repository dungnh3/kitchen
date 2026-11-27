// Package jobstest is an in-memory [jobs.Driver] for tests of code that
// enqueues or runs jobs, without a database.
//
// It follows the same rules as the SQL drivers (claims, fencing, unique
// keys, retention) with the process clock as the database clock, so a test
// can run under testing/synctest. It has no transactions: InsertTx inserts at
// once and a rollback of the surrounding code does not remove the job. Use
// the SQL drivers to test transactional behaviour.
package jobstest

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/dungnh3/kitchen/jobs"
)

// Tx is the transaction type of [Driver]. It carries nothing.
type Tx struct{}

// Driver is an in-memory job store.
type Driver struct {
	mu      sync.Mutex
	next    int64
	jobs    map[int64]*row
	unique  map[string]int64
	wake    chan struct{}
	failing error
}

type row struct {
	jobs.Row
	lockedUntil time.Time
	keep        bool
}

var (
	_ jobs.Driver[Tx] = (*Driver)(nil)
	_ jobs.Waker      = (*Driver)(nil)
)

// New returns an empty Driver.
func New() *Driver {
	return &Driver{jobs: map[int64]*row{}, unique: map[string]int64{}, wake: make(chan struct{}, 1)}
}

// FailWith makes every following call return err until it is called with
// nil. It simulates a database outage.
func (d *Driver) FailWith(err error) {
	d.mu.Lock()
	d.failing = err
	d.mu.Unlock()
}

// Rows returns a copy of every job, ordered by ID.
func (d *Driver) Rows() []jobs.Row {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]jobs.Row, 0, len(d.jobs))
	for _, r := range d.jobs {
		out = append(out, r.Row)
	}
	slices.SortFunc(out, func(a, b jobs.Row) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// Wake implements [jobs.Waker].
func (d *Driver) Wake() <-chan struct{} { return d.wake }

// Insert implements [jobs.Store].
func (d *Driver) Insert(_ context.Context, in []jobs.NewJob) ([]jobs.InsertResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failing != nil {
		return nil, d.failing
	}
	now := time.Now()
	out := make([]jobs.InsertResult, len(in))
	for i, n := range in {
		if n.UniqueKey != "" {
			if id, ok := d.unique[n.UniqueKey]; ok {
				out[i] = jobs.InsertResult{ID: id, Duplicate: true}
				continue
			}
		}
		d.next++
		r := &row{Row: jobs.Row{
			ID: d.next, Kind: n.Kind, Queue: n.Queue, RawArgs: n.Args, Metadata: n.Metadata,
			State: jobs.StateAvailable, Priority: n.Priority, MaxAttempts: n.MaxAttempts,
			ScheduledAt: now.Add(n.Delay), CreatedAt: now, UniqueKey: n.UniqueKey,
		}, keep: n.KeepUnique}
		d.jobs[r.ID] = r
		if n.UniqueKey != "" {
			d.unique[n.UniqueKey] = r.ID
		}
		out[i] = jobs.InsertResult{ID: r.ID}
	}
	select {
	case d.wake <- struct{}{}:
	default:
	}
	return out, nil
}

// InsertTx implements [jobs.Driver]; tx is ignored.
func (d *Driver) InsertTx(ctx context.Context, _ Tx, in []jobs.NewJob) ([]jobs.InsertResult, error) {
	return d.Insert(ctx, in)
}

// Claim implements [jobs.Store].
func (d *Driver) Claim(_ context.Context, queue string, kinds []string, limit int, lease time.Duration) ([]jobs.Claimed, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failing != nil {
		return nil, d.failing
	}
	now := time.Now()
	var due []*row
	for _, r := range d.jobs {
		if r.Queue != queue || !slices.Contains(kinds, r.Kind) {
			continue
		}
		switch {
		case r.State == jobs.StateAvailable && !r.ScheduledAt.After(now):
		case r.State == jobs.StateRunning && r.lockedUntil.Before(now) && r.Attempt < r.MaxAttempts:
		default:
			continue
		}
		due = append(due, r)
	}
	slices.SortFunc(due, func(a, b *row) int {
		return cmp.Or(cmp.Compare(a.Priority, b.Priority), a.ScheduledAt.Compare(b.ScheduledAt), cmp.Compare(a.ID, b.ID))
	})
	due = due[:min(limit, len(due))]
	out := make([]jobs.Claimed, len(due))
	for i, r := range due {
		lag := now.Sub(r.ScheduledAt)
		r.State, r.lockedUntil = jobs.StateRunning, now.Add(lease)
		r.Attempt++
		r.Claim++
		out[i] = jobs.Claimed{Row: r.Row, Lag: max(lag, 0)}
	}
	return out, nil
}

// Extend implements [jobs.Store].
func (d *Driver) Extend(_ context.Context, refs []jobs.Ref, lease time.Duration) ([]jobs.Ref, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failing != nil {
		return nil, d.failing
	}
	var lost []jobs.Ref
	for _, ref := range refs {
		r := d.jobs[ref.ID]
		if r == nil || r.Claim != ref.Claim || r.State != jobs.StateRunning {
			lost = append(lost, ref)
			continue
		}
		r.lockedUntil = time.Now().Add(lease)
	}
	return lost, nil
}

// Settle implements [jobs.Store].
func (d *Driver) Settle(_ context.Context, ref jobs.Ref, s jobs.Settlement) error {
	if err := s.Validate(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failing != nil {
		return d.failing
	}
	r := d.jobs[ref.ID]
	if r == nil || r.Claim != ref.Claim || r.State != jobs.StateRunning {
		return jobs.ErrClaimLost
	}
	now := time.Now()
	r.State, r.lockedUntil = s.State, time.Time{}
	if s.State == jobs.StateAvailable {
		r.ScheduledAt = now.Add(s.Delay)
		if s.GiveBackAttempt {
			r.Attempt--
		}
	} else {
		r.FinalizedAt = now
		d.release(r)
	}
	if s.Error != nil {
		r.Errors = append(r.Errors, *s.Error)
		if len(r.Errors) > 10 {
			r.Errors = r.Errors[len(r.Errors)-10:]
		}
	}
	return nil
}

// release frees the unique key of a finished row unless it is kept.
func (d *Driver) release(r *row) {
	if r.UniqueKey != "" && !r.keep && d.unique[r.UniqueKey] == r.ID {
		delete(d.unique, r.UniqueKey)
	}
}

// Reap implements [jobs.Store].
func (d *Driver) Reap(context.Context) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failing != nil {
		return 0, d.failing
	}
	now, n := time.Now(), 0
	for _, r := range d.jobs {
		if r.State == jobs.StateRunning && r.lockedUntil.Before(now) && r.Attempt >= r.MaxAttempts {
			r.State, r.FinalizedAt, r.lockedUntil = jobs.StateDiscarded, now, time.Time{}
			r.Errors = append(r.Errors, jobs.AttemptError{At: now.UTC(), Attempt: r.Attempt, Error: "lease expired on the last attempt"})
			d.release(r)
			n++
		}
	}
	return n, nil
}

// Cleanup implements [jobs.Store].
func (d *Driver) Cleanup(_ context.Context, p jobs.CleanupParams) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failing != nil {
		return 0, d.failing
	}
	now, n := time.Now(), 0
	var ids []int64
	for id := range d.jobs {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		r := d.jobs[id]
		if n >= p.Limit {
			break
		}
		var age time.Duration
		switch r.State {
		case jobs.StateCompleted:
			age = p.Completed
		case jobs.StateCancelled, jobs.StateDiscarded:
			age = p.Discarded
		default:
			continue
		}
		if now.Sub(r.FinalizedAt) >= age {
			if r.UniqueKey != "" && d.unique[r.UniqueKey] == r.ID {
				delete(d.unique, r.UniqueKey)
			}
			delete(d.jobs, id)
			n++
		}
	}
	return n, nil
}

// Get implements [jobs.Store].
func (d *Driver) Get(_ context.Context, id int64) (*jobs.Row, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failing != nil {
		return nil, d.failing
	}
	r := d.jobs[id]
	if r == nil {
		return nil, jobs.ErrNotFound
	}
	c := r.Row
	return &c, nil
}
