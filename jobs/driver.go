package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrClaimLost is returned by [Store.Settle] when the claim was superseded
// (the lease lapsed and another worker claimed the job) or the row no longer
// exists. The settle changed nothing.
var ErrClaimLost = errors.New("jobs: claim lost")

// ErrNotFound is returned by [Store.Get] for an unknown ID.
var ErrNotFound = errors.New("jobs: job not found")

// NewJob is a job to insert, fully resolved: validated, with defaults
// applied and the args encoded.
type NewJob struct {
	Kind        string
	Queue       string
	Args        []byte // JSON
	Metadata    []byte // JSON object: the trace context of the enqueuer
	Priority    int
	MaxAttempts int
	// Delay is how long after the database's now the job becomes due.
	Delay      time.Duration
	UniqueKey  string // empty: not unique
	KeepUnique bool
}

// Ref names one claim of a job.
type Ref struct {
	ID    int64
	Claim int64
}

// Claimed is a job leased to the caller.
type Claimed struct {
	Row
	// Lag is how long the job waited past its ScheduledAt, by the database
	// clock.
	Lag time.Duration
}

// Ref returns the reference that settles this claim.
func (c Claimed) Ref() Ref { return Ref{ID: c.ID, Claim: c.Claim} }

// Settlement is how a claim ends.
type Settlement struct {
	// State is StateAvailable (retry or snooze), StateCompleted,
	// StateCancelled or StateDiscarded.
	State State
	// Delay is the time until the next attempt (StateAvailable only).
	Delay time.Duration
	// GiveBackAttempt does not count the attempt that is ending
	// (StateAvailable only).
	GiveBackAttempt bool
	// Error is appended to the row's errors, keeping the latest 10.
	Error *AttemptError
}

// Validate reports a settlement a driver must refuse: a state a claim cannot
// end in, a negative delay, or a delay or a given-back attempt on a final
// state.
func (s Settlement) Validate() error {
	switch s.State {
	case StateAvailable:
	case StateCompleted, StateCancelled, StateDiscarded:
		if s.Delay != 0 || s.GiveBackAttempt {
			return fmt.Errorf("jobs: settlement to %q takes no delay or given-back attempt", s.State)
		}
	default:
		return fmt.Errorf("jobs: a claim cannot end in state %q", s.State)
	}
	if s.Delay < 0 {
		return errors.New("jobs: negative settlement delay")
	}
	return nil
}

// CleanupParams selects the finished rows to delete.
type CleanupParams struct {
	// Completed and Discarded are the minimum ages of completed rows and of
	// cancelled and discarded rows.
	Completed, Discarded time.Duration
	// Limit bounds the rows deleted by one call.
	Limit int
}

// Store is the persistence seam of the engine. Implementations are in
// [github.com/dungnh3/kitchen/jobs/pgx], [github.com/dungnh3/kitchen/jobs/mysql]
// and [github.com/dungnh3/kitchen/jobs/jobstest]; the conformance suite in
// jobs/internal/drivertest pins the behaviour below.
//
// Every operation is atomic. All timestamps come from the database clock.
type Store interface {
	// Insert inserts jobs in one transaction of its own. A job whose
	// UniqueKey matches an existing job is not inserted and its result is
	// the existing ID with Duplicate set. Results are in the order of jobs.
	Insert(ctx context.Context, jobs []NewJob) ([]InsertResult, error)

	// Claim leases up to limit due jobs of queue whose kind is in kinds, in
	// order of priority, ScheduledAt, ID, for lease. A job is due when it is
	// available and ScheduledAt has passed, or when it is running, its lease
	// has lapsed and it has attempts left. Claiming adds one to Attempt and
	// to Claim. Concurrent callers never receive the same job.
	Claim(ctx context.Context, queue string, kinds []string, limit int, lease time.Duration) ([]Claimed, error)

	// Extend renews the leases of refs for lease from now. It returns the
	// refs that were not renewed because the claim is no longer the job's
	// latest or the job is not running.
	Extend(ctx context.Context, refs []Ref, lease time.Duration) (lost []Ref, err error)

	// Settle ends a claim. It changes the row only while ref is the row's
	// latest claim and the row is running; otherwise it returns
	// [ErrClaimLost]. Reaching a final state releases the unique key unless
	// KeepUnique was set.
	Settle(ctx context.Context, ref Ref, s Settlement) error

	// Reap discards running jobs whose lease lapsed with no attempts left
	// and returns how many.
	Reap(ctx context.Context) (int, error)

	// Cleanup deletes finished rows older than the retentions, at most
	// p.Limit of them, and returns how many.
	Cleanup(ctx context.Context, p CleanupParams) (int, error)

	// Get returns one job, or [ErrNotFound].
	Get(ctx context.Context, id int64) (*Row, error)
}

// Driver is a [Store] that can also insert inside the caller's transaction.
// TTx is the transaction type of the database library: jackc/pgx's pgx.Tx
// for PostgreSQL, *sql.Tx for MySQL.
type Driver[TTx any] interface {
	Store
	// InsertTx is [Store.Insert] inside tx. It neither commits nor rolls
	// back tx.
	InsertTx(ctx context.Context, tx TTx, jobs []NewJob) ([]InsertResult, error)
}

// Waker is optionally implemented by a Store that can signal new jobs (for
// example PostgreSQL LISTEN/NOTIFY). The client selects on the channel in
// addition to polling.
type Waker interface {
	Wake() <-chan struct{}
}

// Listener is optionally implemented by a Store that needs a goroutine to
// produce wake-ups. The client runs it for the duration of Run.
type Listener interface {
	Listen(ctx context.Context) error
}
