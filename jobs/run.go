package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"go.opentelemetry.io/otel/codes"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/otelx"
	"github.com/dungnh3/kitchen/timex"
)

var errShutdown = errors.New("jobs: shutting down")

// settleTimeout bounds one write that records a job's result. It runs on a
// context detached from shutdown, so results are recorded while stopping.
const settleTimeout = 5 * time.Second

// settleTries is how often a failed settle is repeated before the job is
// left to its lease.
const settleTries = 3

type runningJob struct {
	ref    Ref
	cancel context.CancelCauseFunc
}

func delayUntil(t time.Time) time.Duration {
	if t.IsZero() {
		return 0
	}
	return max(time.Until(t), 0)
}

// signal wakes every fetch loop.
func (c *Client[TTx]) signal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.wakers {
		select {
		case w <- struct{}{}:
		default:
		}
	}
}

// Run works the queues until ctx is cancelled, then stops gracefully: it
// claims nothing more, waits up to [WithShutdownTimeout] for running jobs,
// cancels those still running (they are given back without spending the
// attempt) and returns nil once every goroutine has ended. A worker that
// ignores its context therefore delays Run.
//
// Run can be called once per client.
func (c *Client[TTx]) Run(ctx context.Context) error {
	if c.o.registry == nil || len(c.kinds) == 0 {
		return errors.New("jobs: Run needs a registry with at least one worker (WithRegistry)")
	}
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("jobs: Run called twice")
	}
	c.started = true
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	c.cancel, c.done = cancel, done
	c.wakers = make([]chan struct{}, len(c.o.queues))
	for i := range c.wakers {
		c.wakers[i] = make(chan struct{}, 1)
	}
	wakers := c.wakers
	c.mu.Unlock()
	defer close(done)

	workCtx, cancelWork := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancelWork(nil)

	var loops, jobWG sync.WaitGroup
	if l, ok := c.drv.(Listener); ok {
		loops.Go(func() {
			if err := l.Listen(runCtx); err != nil && runCtx.Err() == nil {
				c.log.ErrorContext(runCtx, "jobs: listener stopped", log.Err(err))
			}
		})
	}
	if w, ok := c.drv.(Waker); ok {
		loops.Go(func() {
			for {
				select {
				case <-runCtx.Done():
					return
				case <-w.Wake():
					c.signal()
				}
			}
		})
	}
	for i, q := range c.o.queues {
		loops.Go(func() { c.fetchLoop(runCtx, workCtx, q, wakers[i], &jobWG) })
	}
	for _, p := range c.o.periodic {
		loops.Go(func() { c.periodicLoop(runCtx, p) })
	}
	loops.Go(func() { c.cleanupLoop(runCtx) })

	hbCtx, stopHB := context.WithCancel(context.WithoutCancel(ctx))
	var hb sync.WaitGroup
	hb.Go(func() { c.heartbeatLoop(hbCtx) })

	<-runCtx.Done()
	loops.Wait()

	jobsDone := make(chan struct{})
	go func() { jobWG.Wait(); close(jobsDone) }()
	t := time.NewTimer(c.o.shutdown)
	select {
	case <-jobsDone:
	case <-t.C:
		c.log.WarnContext(ctx, "jobs: shutdown timeout, cancelling running jobs", log.Duration("timeout", c.o.shutdown))
		cancelWork(errShutdown)
		<-jobsDone
	}
	t.Stop()
	stopHB()
	hb.Wait()
	return nil
}

// Close stops a client that is running (see [Client.Run]) and waits for it
// to return or for ctx. It is a no-op when Run was never called.
func (c *Client[TTx]) Close(ctx context.Context) error {
	c.mu.Lock()
	cancel, done := c.cancel, c.done
	c.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client[TTx]) fetchLoop(ctx, workCtx context.Context, q Queue, wake <-chan struct{}, jobWG *sync.WaitGroup) {
	var (
		mu       sync.Mutex
		inflight int
	)
	freed := make(chan struct{}, 1)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-wake:
		case <-freed:
		}
		for ctx.Err() == nil {
			mu.Lock()
			free := q.Workers - inflight
			mu.Unlock()
			if free <= 0 {
				break
			}
			claimed, err := c.drv.Claim(ctx, q.Name, c.kinds, free, c.o.lease)
			if err != nil {
				if ctx.Err() == nil {
					c.log.ErrorContext(ctx, "jobs: claim failed", log.String("queue", q.Name), log.Err(err))
				}
				break
			}
			for _, cl := range claimed {
				mu.Lock()
				inflight++
				mu.Unlock()
				jobWG.Go(func() {
					defer func() {
						mu.Lock()
						inflight--
						mu.Unlock()
						select {
						case freed <- struct{}{}:
						default:
						}
					}()
					c.execute(workCtx, cl)
				})
			}
			if len(claimed) < free {
				break
			}
		}
		timer.Reset(c.o.poll)
	}
}

// execute runs one claimed job and records its result.
func (c *Client[TTx]) execute(workCtx context.Context, cl Claimed) {
	e := c.o.registry.get(cl.Kind)
	if e == nil { // not reachable: claims are filtered by the registry's kinds
		c.settle(workCtx, cl, Settlement{State: StateCancelled, Error: newAttemptError(time.Now(), cl.Attempt, fmt.Errorf("no worker for kind %q", cl.Kind))}, outcomeCancelled, 0)
		return
	}
	jctx, cancelJob := context.WithCancelCause(workCtx)
	defer cancelJob(nil)
	c.mu.Lock()
	c.running[cl.ID] = runningJob{ref: cl.Ref(), cancel: cancelJob}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.running, cl.ID)
		c.mu.Unlock()
	}()

	timeout := e.timeout
	if timeout == 0 {
		timeout = c.o.jobTimeout
	}
	wctx := jctx
	if timeout > 0 {
		var cancelT context.CancelFunc
		wctx, cancelT = context.WithTimeout(jctx, timeout)
		defer cancelT()
	}
	var meta map[string]string
	_ = json.Unmarshal(cl.Metadata, &meta) // a malformed value only loses the trace link
	wctx, span := c.tel.startWork(wctx, cl, meta)
	c.tel.lag.Record(wctx, cl.Lag.Seconds(), metricAttrs(cl))

	start := time.Now()
	err := c.callWork(wctx, e, cl)
	dur := time.Since(start)

	if errors.Is(context.Cause(jctx), ErrClaimLost) {
		span.SetStatus(codes.Error, "claim lost")
		span.End()
		c.tel.recordFinish(workCtx, cl, outcomeLost, dur)
		c.log.WarnContext(wctx, "jobs: lease lost while running; the result is dropped", jobAttrs2(cl)...)
		return
	}
	s, outcome := c.decide(cl, err, errors.Is(context.Cause(workCtx), errShutdown))
	if err != nil && outcome != outcomeSnoozed {
		otelx.RecordError(span, err)
		span.SetStatus(codes.Error, outcome)
	}
	span.End()
	c.settle(workCtx, cl, s, outcome, dur)
	if err != nil {
		c.logFailure(workCtx, cl, outcome, err)
	}
}

// callWork calls the worker and turns a panic into an error.
func (c *Client[TTx]) callWork(ctx context.Context, e *entry, cl Claimed) (err error) {
	defer func() {
		if r := recover(); r != nil {
			c.log.ErrorContext(ctx, "jobs: worker panicked", append(jobAttrs2(cl),
				log.Any("panic", r), log.String("stack", string(debug.Stack())))...)
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return e.work(ctx, cl.Row)
}

// decide maps a worker's result to the settlement and the outcome label.
func (c *Client[TTx]) decide(cl Claimed, err error, shutdown bool) (Settlement, string) {
	now := time.Now()
	var (
		ce *cancelError
		se *snoozeError
	)
	switch {
	case err == nil:
		return Settlement{State: StateCompleted}, outcomeCompleted
	case errors.As(err, &ce):
		return Settlement{State: StateCancelled, Error: newAttemptError(now, cl.Attempt, err)}, outcomeCancelled
	case errors.As(err, &se):
		return Settlement{State: StateAvailable, Delay: se.d, GiveBackAttempt: true}, outcomeSnoozed
	case shutdown && errors.Is(err, context.Canceled):
		return Settlement{State: StateAvailable, GiveBackAttempt: true}, outcomeReleased
	case cl.Attempt >= cl.MaxAttempts:
		return Settlement{State: StateDiscarded, Error: newAttemptError(now, cl.Attempt, err)}, outcomeDiscarded
	default:
		return Settlement{
			State: StateAvailable, Delay: timex.Backoff(c.o.backoffBase, c.o.backoffMax, cl.Attempt),
			Error: newAttemptError(now, cl.Attempt, err),
		}, outcomeRetried
	}
}

// settle records the result on a context that outlives shutdown.
func (c *Client[TTx]) settle(workCtx context.Context, cl Claimed, s Settlement, outcome string, d time.Duration) {
	ctx := context.WithoutCancel(workCtx)
	var err error
	for try := 1; try <= settleTries; try++ {
		sctx, cancel := context.WithTimeout(ctx, settleTimeout)
		err = c.drv.Settle(sctx, cl.Ref(), s)
		cancel()
		if err == nil || errors.Is(err, ErrClaimLost) || try == settleTries {
			break
		}
		time.Sleep(time.Duration(try) * 200 * time.Millisecond)
	}
	switch {
	case err == nil:
		c.tel.recordFinish(ctx, cl, outcome, d)
		if s.State == StateAvailable {
			c.signal()
		}
	case errors.Is(err, ErrClaimLost):
		c.tel.recordFinish(ctx, cl, outcomeLost, d)
		c.log.WarnContext(ctx, "jobs: claim lost before the result was recorded", jobAttrs2(cl)...)
	default:
		c.log.ErrorContext(ctx, "jobs: recording the result failed; the job runs again when its lease lapses",
			append(jobAttrs2(cl), log.Err(err))...)
	}
}

func (c *Client[TTx]) logFailure(ctx context.Context, cl Claimed, outcome string, err error) {
	attrs := append(jobAttrs2(cl), log.String("outcome", outcome), log.Err(err))
	switch outcome {
	case outcomeDiscarded:
		c.log.ErrorContext(ctx, "jobs: job discarded after its last attempt", attrs...)
	case outcomeRetried, outcomeCancelled:
		c.log.WarnContext(ctx, "jobs: attempt failed", attrs...)
	default:
		c.log.InfoContext(ctx, "jobs: attempt ended early", attrs...)
	}
}

// heartbeatLoop renews the leases of the running jobs every lease/3 and
// cancels the jobs whose claim is lost.
func (c *Client[TTx]) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(c.o.lease / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.mu.Lock()
		refs := make([]Ref, 0, len(c.running))
		for _, r := range c.running {
			refs = append(refs, r.ref)
		}
		c.mu.Unlock()
		if len(refs) == 0 {
			continue
		}
		lost, err := c.drv.Extend(ctx, refs, c.o.lease)
		if err != nil {
			if ctx.Err() == nil {
				c.log.WarnContext(ctx, "jobs: renewing leases failed", log.Int("jobs", len(refs)), log.Err(err))
			}
			continue
		}
		c.mu.Lock()
		for _, ref := range lost {
			if r, ok := c.running[ref.ID]; ok && r.ref == ref {
				r.cancel(ErrClaimLost)
			}
		}
		c.mu.Unlock()
	}
}

// periodicLoop inserts the job of each upcoming slot. Every replica does it
// and the unique key lets exactly one insert win.
func (c *Client[TTx]) periodicLoop(ctx context.Context, p Periodic) {
	for ctx.Err() == nil {
		now := time.Now()
		next := time.Unix(0, (now.UnixNano()/int64(p.Interval)+1)*int64(p.Interval))
		args := p.New()
		opts := InsertOpts{ScheduledAt: next, KeepUnique: true}
		if p.Opts != nil {
			opts = *p.Opts
			opts.ScheduledAt, opts.KeepUnique = next, true
		}
		if args != nil {
			opts.UniqueKey = fmt.Sprintf("periodic:%s:%d", args.Kind(), next.Unix())
		}
		if _, err := c.Insert(ctx, args, &opts); err != nil {
			if ctx.Err() != nil {
				return
			}
			c.log.ErrorContext(ctx, "jobs: inserting a periodic job failed", log.Err(err))
			if !sleep(ctx, min(p.Interval, 5*time.Second)) {
				return
			}
			continue
		}
		if !sleep(ctx, time.Until(next)) {
			return
		}
	}
}

// cleanupLoop reaps jobs whose last attempt lost its lease and deletes
// expired rows, in batches, until none is left.
func (c *Client[TTx]) cleanupLoop(ctx context.Context) {
	const batch = 1000
	for {
		for ctx.Err() == nil {
			n, err := c.drv.Reap(ctx)
			if err != nil {
				if ctx.Err() == nil {
					c.log.WarnContext(ctx, "jobs: reaping failed", log.Err(err))
				}
				break
			}
			if n > 0 {
				c.log.ErrorContext(ctx, "jobs: jobs discarded: the lease lapsed on their last attempt", log.Int("jobs", n))
			}
			n, err = c.drv.Cleanup(ctx, CleanupParams{Completed: c.o.retainDone, Discarded: c.o.retainDead, Limit: batch})
			if err != nil {
				if ctx.Err() == nil {
					c.log.WarnContext(ctx, "jobs: clean-up failed", log.Err(err))
				}
				break
			}
			if n < batch {
				break
			}
		}
		if !sleep(ctx, c.o.cleanupEach) {
			return
		}
	}
}

// sleep waits d or until ctx is done; it reports whether d elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(max(d, 0))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
