// Package jobs is a durable background job queue stored in the service's own
// SQL database (PostgreSQL through [github.com/dungnh3/kitchen/jobs/pgx],
// MySQL 8 through [github.com/dungnh3/kitchen/jobs/mysql]).
//
// A job is a row. [Client.InsertTx] writes it in the same transaction as the
// business data, so the job exists if and only if the transaction committed.
// A worker process runs [Client.Run], which claims due rows, runs the
// registered [Worker] and records the result. Execution is at least once:
// workers must be idempotent.
//
// Claims are leases. A claimed job is leased for [WithLease]; a heartbeat
// renews the lease while the job runs. A process that crashes stops renewing,
// the lease lapses and another worker claims the job again (the attempt
// counts). Every write that settles a claim carries the claim's fencing
// counter, so a worker whose lease lapsed cannot overwrite the result of the
// worker that claimed the job after it. There is no leader and no extra
// service: periodic jobs and clean-up are idempotent and run on every
// replica.
//
// A worker's result decides what happens next:
//
//   - nil: the job is completed;
//   - [Cancel](err): the job is cancelled, without retry;
//   - [Snooze](d): the job runs again after d and the attempt is not counted;
//   - any other error, or a panic: the job is retried after an exponential
//     backoff ([WithRetryBackoff]) until [WithMaxAttempts], then discarded.
//     Cancelled and discarded jobs are kept for [WithRetention], for
//     inspection.
//
// All times of the queue are the database clock; the process clock only
// computes delays.
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"
)

// State is the state of a job row.
type State string

// The job states.
const (
	// StateAvailable: waiting to run at ScheduledAt.
	StateAvailable State = "available"
	// StateRunning: claimed by a worker until LockedUntil.
	StateRunning State = "running"
	// StateCompleted: finished successfully.
	StateCompleted State = "completed"
	// StateCancelled: stopped on purpose (see [Cancel]); never retried.
	StateCancelled State = "cancelled"
	// StateDiscarded: out of attempts. This is the dead-letter state.
	StateDiscarded State = "discarded"
)

// Final reports whether s is a state a job never leaves.
func (s State) Final() bool {
	return s == StateCompleted || s == StateCancelled || s == StateDiscarded
}

// Args is the payload of a job. It is stored as JSON. Kind names the job in
// the database and must be stable: renaming it orphans queued jobs. Declare
// Kind on the value (not pointer) receiver.
//
// Args may also implement [InsertDefaults] to set the queue, priority or
// attempts of its jobs.
type Args interface {
	Kind() string
}

// InsertDefaults is implemented by [Args] that carry insert defaults. The
// options of the insert call override the non-zero fields.
type InsertDefaults interface {
	InsertOpts() InsertOpts
}

// InsertOpts are per-job options. The zero value means the defaults.
type InsertOpts struct {
	// Queue is the queue name (default "default").
	Queue string
	// Priority orders claims within a queue: lower runs first (default 1).
	Priority int
	// MaxAttempts is how many times the job may run (default
	// [WithMaxAttempts]).
	MaxAttempts int
	// ScheduledAt is the earliest start (default: now).
	ScheduledAt time.Time
	// UniqueKey makes the insert a no-op while a job with the same key
	// exists (see [InsertResult.Duplicate]). The key is released when the
	// job reaches a final state, unless KeepUnique is set. At most 255 bytes.
	UniqueKey string
	// KeepUnique keeps UniqueKey until the finished job is deleted by the
	// retention clean-up.
	KeepUnique bool
}

// InsertParams is one job of an [Client.InsertMany] call.
type InsertParams struct {
	Args Args
	Opts *InsertOpts
}

// InsertResult is the outcome of an insert.
type InsertResult struct {
	// ID is the job's ID; for a duplicate it is the ID of the existing job.
	ID int64
	// Duplicate is true when UniqueKey matched an existing job and nothing
	// was inserted.
	Duplicate bool
}

// AttemptError records one failed attempt.
type AttemptError struct {
	At      time.Time `json:"at"`
	Attempt int       `json:"attempt"`
	Error   string    `json:"error"`
}

// maxErrorBytes bounds the stored message of an attempt.
const maxErrorBytes = 2048

func newAttemptError(at time.Time, attempt int, err error) *AttemptError {
	msg := err.Error()
	if len(msg) > maxErrorBytes {
		msg = msg[:maxErrorBytes]
		for len(msg) > 0 && !utf8.ValidString(msg) {
			msg = msg[:len(msg)-1]
		}
	}
	return &AttemptError{At: at.UTC(), Attempt: attempt, Error: msg}
}

// Row is a job as stored.
type Row struct {
	ID          int64
	Kind        string
	Queue       string
	RawArgs     json.RawMessage
	Metadata    json.RawMessage
	State       State
	Priority    int
	Attempt     int
	MaxAttempts int
	ScheduledAt time.Time
	CreatedAt   time.Time
	FinalizedAt time.Time // zero until the job is final
	// Claim is the fencing counter: it grows on every claim.
	Claim     int64
	UniqueKey string
	Errors    []AttemptError
}

// Job is a claimed job as the worker sees it.
type Job[T Args] struct {
	Row
	// Args is the decoded payload.
	Args T
}

// Worker runs the jobs of one kind. Work must be idempotent and must return
// when ctx is done. A Worker may also implement
//
//	Timeout() time.Duration
//
// to bound one attempt (default [WithJobTimeout], 1 minute; a negative value
// means no timeout).
type Worker[T Args] interface {
	Work(ctx context.Context, job *Job[T]) error
}

// WorkFunc adapts a function to a [Worker].
type WorkFunc[T Args] func(ctx context.Context, job *Job[T]) error

// Work calls f.
func (f WorkFunc[T]) Work(ctx context.Context, job *Job[T]) error { return f(ctx, job) }

type cancelError struct{ err error }

func (e *cancelError) Error() string { return "job cancelled: " + e.err.Error() }
func (e *cancelError) Unwrap() error { return e.err }

// Cancel makes a worker's result permanent: the job becomes cancelled (with
// err recorded) and is not retried. A nil err records a generic message.
func Cancel(err error) error {
	if err == nil {
		err = errors.New("cancelled")
	}
	return &cancelError{err: err}
}

type snoozeError struct{ d time.Duration }

func (e *snoozeError) Error() string { return fmt.Sprintf("job snoozed for %s", e.d) }

// Snooze makes a worker's result "run again after d": the job becomes
// available at now+d and the attempt is given back, so snoozing never uses
// up MaxAttempts. A negative d counts as 0.
func Snooze(d time.Duration) error { return &snoozeError{d: max(d, 0)} }

var (
	queueRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	kindRE  = regexp.MustCompile(`^[A-Za-z0-9_.:/-]{1,255}$`)
)
