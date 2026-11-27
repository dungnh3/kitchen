package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/jobs"
	"github.com/dungnh3/kitchen/jobs/jobstest"
	"github.com/dungnh3/kitchen/log"
)

type echo struct {
	N int `json:"n"`
}

func (echo) Kind() string { return "echo" }

type tick struct{}

func (tick) Kind() string { return "tick" }

type orphan struct{}

func (orphan) Kind() string { return "orphan" }

// register adds a worker for T that calls fn.
func register[T jobs.Args](t *testing.T, reg *jobs.Registry, fn func(context.Context, *jobs.Job[T]) error) {
	t.Helper()
	if err := jobs.Register(reg, jobs.WorkFunc[T](fn)); err != nil {
		t.Fatal(err)
	}
}

func newClient(t *testing.T, drv jobs.Driver[jobstest.Tx], reg *jobs.Registry, opts ...jobs.Option) *jobs.Client[jobstest.Tx] {
	t.Helper()
	opts = append([]jobs.Option{jobs.WithLogger(log.Discard()), jobs.WithRegistry(reg)}, opts...)
	c, err := jobs.NewClient(drv, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// start runs c and returns the function that stops it.
func start(t *testing.T, c *jobs.Client[jobstest.Tx]) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	return func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run = %v", err)
		}
	}
}

func row(t *testing.T, drv *jobstest.Driver, id int64) jobs.Row {
	t.Helper()
	r, err := drv.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return *r
}

func TestRunCompletesJobs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		var sum atomic.Int64
		register(t, reg, func(_ context.Context, j *jobs.Job[echo]) error {
			sum.Add(int64(j.Args.N))
			if j.Kind != "echo" || j.Attempt != 1 || j.Queue != "default" {
				t.Errorf("job = %+v", j.Row)
			}
			return nil
		})
		c := newClient(t, drv, reg)
		stop := start(t, c)
		defer stop()
		for i := 1; i <= 3; i++ {
			if _, err := c.Insert(context.Background(), echo{N: i}, nil); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if sum.Load() != 6 {
			t.Fatalf("sum = %d", sum.Load())
		}
		for _, r := range drv.Rows() {
			if r.State != jobs.StateCompleted || r.Attempt != 1 || r.FinalizedAt.IsZero() {
				t.Fatalf("row = %+v", r)
			}
		}
	})
}

func TestInsertWakesAnIdleQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		var ran atomic.Bool
		register(t, reg, func(context.Context, *jobs.Job[echo]) error { ran.Store(true); return nil })
		c := newClient(t, drv, reg, jobs.WithPollInterval(time.Hour))
		stop := start(t, c)
		defer stop()
		synctest.Wait()
		before := time.Now()
		if _, err := c.Insert(context.Background(), echo{}, nil); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if !ran.Load() || !time.Now().Equal(before) {
			t.Fatalf("ran = %v after %v: an insert must not wait for the poll", ran.Load(), time.Since(before))
		}
	})
}

func TestResults(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name        string
		work        func(attempt int) error
		maxAttempts int
		wantState   jobs.State
		wantAttempt int
		wantErrs    int
		wantInErr   string
	}{
		{"success", func(int) error { return nil }, 3, jobs.StateCompleted, 1, 0, ""},
		{"retry then success", func(a int) error {
			if a < 3 {
				return boom
			}
			return nil
		}, 5, jobs.StateCompleted, 3, 2, "boom"},
		{"out of attempts is discarded", func(int) error { return boom }, 3, jobs.StateDiscarded, 3, 3, "boom"},
		{"cancel is final at once", func(int) error { return jobs.Cancel(boom) }, 3, jobs.StateCancelled, 1, 1, "boom"},
		{"wrapped cancel", func(int) error { return fmt.Errorf("wrap: %w", jobs.Cancel(boom)) }, 3, jobs.StateCancelled, 1, 1, "boom"},
		{"snooze does not spend attempts", func(a int) error {
			if a <= 5 { // attempt stays 1 while snoozing
				return jobs.Snooze(time.Minute)
			}
			return nil
		}, 1, jobs.StateCompleted, 1, 0, ""},
		{"panic is a failed attempt", func(a int) error {
			if a == 1 {
				panic("kaboom")
			}
			return nil
		}, 3, jobs.StateCompleted, 2, 1, "panic: kaboom"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				drv := jobstest.New()
				reg := jobs.NewRegistry()
				var calls atomic.Int64
				register(t, reg, func(_ context.Context, j *jobs.Job[echo]) error {
					n := int(calls.Add(1))
					attempt := j.Attempt
					if tc.name == "snooze does not spend attempts" {
						attempt = n
					}
					return tc.work(attempt)
				})
				c := newClient(t, drv, reg, jobs.WithRetryBackoff(time.Second, time.Minute))
				stop := start(t, c)
				defer stop()
				res, err := c.Insert(context.Background(), echo{}, &jobs.InsertOpts{MaxAttempts: tc.maxAttempts})
				if err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Hour)
				synctest.Wait()
				r := row(t, drv, res.ID)
				if r.State != tc.wantState || r.Attempt != tc.wantAttempt || len(r.Errors) != tc.wantErrs {
					t.Fatalf("row = %+v", r)
				}
				if tc.wantInErr != "" && !strings.Contains(r.Errors[len(r.Errors)-1].Error, tc.wantInErr) {
					t.Fatalf("errors = %+v", r.Errors)
				}
			})
		})
	}
}

func TestRetryBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		var times []time.Time
		register(t, reg, func(context.Context, *jobs.Job[echo]) error {
			times = append(times, time.Now())
			return errors.New("fail")
		})
		c := newClient(t, drv, reg, jobs.WithRetryBackoff(10*time.Second, 25*time.Second), jobs.WithPollInterval(100*time.Millisecond))
		stop := start(t, c)
		defer stop()
		if _, err := c.Insert(context.Background(), echo{}, &jobs.InsertOpts{MaxAttempts: 5}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Minute)
		synctest.Wait()
		if len(times) != 5 {
			t.Fatalf("ran %d times", len(times))
		}
		// The delay before attempt n+1 is within [d/2, d] of 10s, 20s, 25s, 25s (equal jitter).
		for i, d := range []time.Duration{10 * time.Second, 20 * time.Second, 25 * time.Second, 25 * time.Second} {
			gap := times[i+1].Sub(times[i])
			if gap < d/2 || gap > d+200*time.Millisecond {
				t.Errorf("gap before attempt %d = %v, want within [%v, %v]", i+2, gap, d/2, d)
			}
		}
	})
}

func TestOnlyRegisteredKindsAreClaimed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		register(t, reg, func(context.Context, *jobs.Job[echo]) error { return nil })
		c := newClient(t, drv, reg)
		stop := start(t, c)
		defer stop()
		res, err := c.Insert(context.Background(), orphan{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if r := row(t, drv, res.ID); r.State != jobs.StateAvailable || r.Attempt != 0 {
			t.Fatalf("a job without a worker was claimed: %+v", r)
		}
	})
}

func TestUndecodableArgsAreCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		register(t, reg, func(context.Context, *jobs.Job[echo]) error { t.Error("worker called"); return nil })
		c := newClient(t, drv, reg)
		stop := start(t, c)
		defer stop()
		ids, err := drv.Insert(context.Background(), []jobs.NewJob{{Kind: "echo", Queue: "default", Args: []byte(`{"n":"x"}`), Metadata: []byte(`{}`), Priority: 1, MaxAttempts: 3}})
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		r := row(t, drv, ids[0].ID)
		if r.State != jobs.StateCancelled || len(r.Errors) != 1 || !strings.Contains(r.Errors[0].Error, "decode args") {
			t.Fatalf("row = %+v", r)
		}
	})
}

func TestQueueConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		var cur, peak atomic.Int64
		release := make(chan struct{})
		register(t, reg, func(ctx context.Context, _ *jobs.Job[echo]) error {
			n := cur.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			defer cur.Add(-1)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		c := newClient(t, drv, reg, jobs.WithQueues(jobs.Queue{Name: "default", Workers: 2}))
		stop := start(t, c)
		defer stop()
		for range 5 {
			if _, err := c.Insert(context.Background(), echo{}, nil); err != nil {
				t.Fatal(err)
			}
		}
		synctest.Wait()
		if cur.Load() != 2 {
			t.Fatalf("running = %d, want 2", cur.Load())
		}
		close(release)
		synctest.Wait()
		if peak.Load() != 2 {
			t.Fatalf("peak concurrency = %d, want 2", peak.Load())
		}
		for _, r := range drv.Rows() {
			if r.State != jobs.StateCompleted {
				t.Fatalf("row = %+v", r)
			}
		}
	})
}

func TestPriorityOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		var mu sync.Mutex
		var order []int
		register(t, reg, func(_ context.Context, j *jobs.Job[echo]) error {
			mu.Lock()
			order = append(order, j.Args.N)
			mu.Unlock()
			return nil
		})
		c := newClient(t, drv, reg, jobs.WithQueues(jobs.Queue{Name: "default", Workers: 1}))
		// Insert before Run so every job is due at the first claim.
		for _, p := range []int{3, 1, 2} {
			if _, err := c.Insert(context.Background(), echo{N: p}, &jobs.InsertOpts{Priority: p}); err != nil {
				t.Fatal(err)
			}
		}
		stop := start(t, c)
		defer stop()
		synctest.Wait()
		if fmt.Sprint(order) != "[1 2 3]" {
			t.Fatalf("order = %v", order)
		}
	})
}

func TestScheduledAt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		var ranAt atomic.Value
		register(t, reg, func(context.Context, *jobs.Job[echo]) error { ranAt.Store(time.Now()); return nil })
		c := newClient(t, drv, reg)
		stop := start(t, c)
		defer stop()
		at := time.Now().Add(90 * time.Second)
		if _, err := c.Insert(context.Background(), echo{}, &jobs.InsertOpts{ScheduledAt: at}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Hour)
		got, _ := ranAt.Load().(time.Time)
		if got.Before(at) || got.Sub(at) > 2*time.Second {
			t.Fatalf("ran at %v, scheduled at %v", got, at)
		}
	})
}

func TestShutdownWaitsForRunningJobs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		started := make(chan struct{})
		register(t, reg, func(ctx context.Context, _ *jobs.Job[echo]) error {
			close(started)
			select {
			case <-time.After(5 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		c := newClient(t, drv, reg, jobs.WithShutdownTimeout(30*time.Second))
		res, err := c.Insert(context.Background(), echo{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()
		<-started
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if r := row(t, drv, res.ID); r.State != jobs.StateCompleted {
			t.Fatalf("a job that finished inside the shutdown timeout is %+v", r)
		}
	})
}

func TestShutdownGivesBackJobsThatOverrun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		started := make(chan struct{})
		register(t, reg, func(ctx context.Context, _ *jobs.Job[echo]) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
		c := newClient(t, drv, reg, jobs.WithShutdownTimeout(10*time.Second), jobs.WithJobTimeout(0))
		res, err := c.Insert(context.Background(), echo{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()
		<-started
		begin := time.Now()
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if d := time.Since(begin); d != 10*time.Second {
			t.Fatalf("Run returned after %v, want the shutdown timeout", d)
		}
		r := row(t, drv, res.ID)
		if r.State != jobs.StateAvailable || r.Attempt != 0 || len(r.Errors) != 0 {
			t.Fatalf("an interrupted job must be available again without spending its attempt: %+v", r)
		}
	})
}

func TestCloseStopsRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		register(t, reg, func(context.Context, *jobs.Job[echo]) error { return nil })
		c := newClient(t, drv, reg)
		if err := c.Close(context.Background()); err != nil {
			t.Fatalf("Close before Run = %v", err)
		}
		done := make(chan error, 1)
		go func() { done <- c.Run(context.Background()) }()
		synctest.Wait()
		if err := c.Run(context.Background()); err == nil {
			t.Fatal("a second Run was accepted")
		}
		if err := c.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestRunNeedsAWorker(t *testing.T) {
	c, err := jobs.NewClient(jobstest.New(), jobs.WithLogger(log.Discard()))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Run(context.Background()); err == nil {
		t.Fatal("Run without a registry succeeded")
	}
	// An insert-only client still inserts.
	if _, err := c.Insert(context.Background(), echo{}, nil); err != nil {
		t.Fatal(err)
	}
}

// lostExtend loses every lease at its next renewal.
type lostExtend struct{ *jobstest.Driver }

func (l lostExtend) Extend(_ context.Context, refs []jobs.Ref, _ time.Duration) ([]jobs.Ref, error) {
	return refs, nil
}

func TestLostLeaseCancelsTheJobAndDropsItsResult(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		var runs atomic.Int64
		var cause atomic.Value
		register(t, reg, func(ctx context.Context, _ *jobs.Job[echo]) error {
			if runs.Add(1) == 1 {
				<-ctx.Done()
				cause.Store(context.Cause(ctx))
				return nil // would complete the job, but the claim is gone
			}
			return nil
		})
		c := newClient(t, lostExtend{drv}, reg, jobs.WithLease(3*time.Second))
		stop := start(t, c)
		defer stop()
		res, err := c.Insert(context.Background(), echo{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second) // one heartbeat (every second), lease not yet lapsed
		synctest.Wait()
		if err, _ := cause.Load().(error); !errors.Is(err, jobs.ErrClaimLost) {
			t.Fatalf("cause = %v", err)
		}
		if r := row(t, drv, res.ID); r.State != jobs.StateRunning {
			t.Fatalf("the dropped result was recorded: %+v", r)
		}
		// The lease lapses, the job is claimed again and completes.
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if r := row(t, drv, res.ID); r.State != jobs.StateCompleted || r.Attempt != 2 || runs.Load() != 2 {
			t.Fatalf("row = %+v after %d runs", r, runs.Load())
		}
	})
}

func TestHeartbeatKeepsALongJob(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		var runs atomic.Int64
		register(t, reg, func(ctx context.Context, _ *jobs.Job[echo]) error {
			runs.Add(1)
			select {
			case <-time.After(5 * time.Minute): // ten times the lease
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		c := newClient(t, drv, reg, jobs.WithLease(30*time.Second), jobs.WithJobTimeout(time.Hour))
		stop := start(t, c)
		defer stop()
		res, err := c.Insert(context.Background(), echo{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(6 * time.Minute)
		synctest.Wait()
		if r := row(t, drv, res.ID); r.State != jobs.StateCompleted || r.Attempt != 1 || runs.Load() != 1 {
			t.Fatalf("a renewed job ran %d times: %+v", runs.Load(), r)
		}
	})
}

func TestCrashedWorkersJobIsClaimedAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		var mu sync.Mutex
		var attempts []int
		seen := func() []int { mu.Lock(); defer mu.Unlock(); return slices.Clone(attempts) }
		register(t, reg, func(_ context.Context, j *jobs.Job[echo]) error {
			mu.Lock()
			defer mu.Unlock()
			attempts = append(attempts, j.Attempt)
			return nil
		})
		c := newClient(t, drv, reg, jobs.WithLease(30*time.Second))
		res, err := c.Insert(context.Background(), echo{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Another process claims the job and dies without settling it.
		if got, err := drv.Claim(context.Background(), "default", []string{"echo"}, 1, 30*time.Second); err != nil || len(got) != 1 {
			t.Fatalf("Claim = %v, %v", got, err)
		}
		stop := start(t, c)
		defer stop()
		time.Sleep(20 * time.Second)
		synctest.Wait()
		if len(seen()) != 0 {
			t.Fatal("ran a job whose lease is alive")
		}
		time.Sleep(15 * time.Second)
		synctest.Wait()
		if fmt.Sprint(seen()) != "[2]" || row(t, drv, res.ID).State != jobs.StateCompleted {
			t.Fatalf("attempts = %v, row = %+v", seen(), row(t, drv, res.ID))
		}
	})
}

func TestPeriodicRunsOncePerSlotAcrossReplicas(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		var runs atomic.Int64
		var stops []func()
		for range 2 { // two replicas of one service
			reg := jobs.NewRegistry()
			register(t, reg, func(context.Context, *jobs.Job[tick]) error { runs.Add(1); return nil })
			c := newClient(t, drv, reg, jobs.WithPeriodic(jobs.Every(time.Minute, func() jobs.Args { return tick{} })))
			stops = append(stops, start(t, c))
		}
		time.Sleep(5*time.Minute + 30*time.Second)
		synctest.Wait()
		for _, stop := range stops {
			stop()
		}
		if runs.Load() != 5 {
			t.Fatalf("runs = %d, want 5 (slots at 1m..5m)", runs.Load())
		}
		// The sixth slot's job is already queued, once.
		keys := map[string]bool{}
		for _, r := range drv.Rows() {
			if keys[r.UniqueKey] {
				t.Fatalf("slot %q inserted twice", r.UniqueKey)
			}
			keys[r.UniqueKey] = true
		}
		if len(drv.Rows()) != 6 {
			t.Fatalf("rows = %d, want 6", len(drv.Rows()))
		}
	})
}

func TestPeriodicRunsAreRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		var runs atomic.Int64
		register(t, reg, func(context.Context, *jobs.Job[tick]) error {
			if runs.Add(1) == 1 {
				return errors.New("transient")
			}
			return nil
		})
		c := newClient(t, drv, reg, jobs.WithRetryBackoff(time.Second, time.Second),
			jobs.WithPeriodic(jobs.Every(time.Hour, func() jobs.Args { return tick{} })))
		stop := start(t, c)
		defer stop()
		time.Sleep(time.Hour + time.Minute)
		synctest.Wait()
		rows := drv.Rows()
		if runs.Load() != 2 || rows[0].State != jobs.StateCompleted || rows[0].Attempt != 2 {
			t.Fatalf("runs = %d, row = %+v", runs.Load(), rows[0])
		}
	})
}

func TestCleanupDeletesExpiredJobs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		register(t, reg, func(context.Context, *jobs.Job[echo]) error { return nil })
		register(t, reg, func(context.Context, *jobs.Job[tick]) error { return jobs.Cancel(errors.New("no")) })
		c := newClient(t, drv, reg, jobs.WithRetention(time.Hour, 3*time.Hour))
		stop := start(t, c)
		defer stop()
		done, _ := c.Insert(context.Background(), echo{}, nil)
		dead, _ := c.Insert(context.Background(), tick{}, nil)
		time.Sleep(30 * time.Minute)
		if len(drv.Rows()) != 2 {
			t.Fatal("rows deleted before their retention")
		}
		time.Sleep(45 * time.Minute) // 1h15m: completed is expired, cancelled is not
		synctest.Wait()
		if _, err := drv.Get(context.Background(), done.ID); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("completed job kept: %v", err)
		}
		if _, err := drv.Get(context.Background(), dead.ID); err != nil {
			t.Fatalf("cancelled job deleted early: %v", err)
		}
		time.Sleep(2 * time.Hour)
		synctest.Wait()
		if len(drv.Rows()) != 0 {
			t.Fatalf("rows left: %+v", drv.Rows())
		}
	})
}

func TestOutageIsSurvived(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		register(t, reg, func(context.Context, *jobs.Job[echo]) error { return nil })
		c := newClient(t, drv, reg)
		res, err := c.Insert(context.Background(), echo{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		drv.FailWith(errors.New("db down"))
		stop := start(t, c)
		defer stop()
		time.Sleep(time.Minute)
		synctest.Wait()
		drv.FailWith(nil)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if r := row(t, drv, res.ID); r.State != jobs.StateCompleted {
			t.Fatalf("row = %+v", r)
		}
	})
}

func TestUniqueInsert(t *testing.T) {
	drv := jobstest.New()
	c, err := jobs.NewClient(drv, jobs.WithLogger(log.Discard()))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	first, err := c.Insert(ctx, echo{}, &jobs.InsertOpts{UniqueKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	dup, err := c.InsertTx(ctx, jobstest.Tx{}, echo{}, &jobs.InsertOpts{UniqueKey: "k"})
	if err != nil || !dup.Duplicate || dup.ID != first.ID || first.Duplicate {
		t.Fatalf("first %+v dup %+v err %v", first, dup, err)
	}
	if n := len(drv.Rows()); n != 1 {
		t.Fatalf("rows = %d", n)
	}
}

type withDefaults struct{}

func (withDefaults) Kind() string { return "with_defaults" }
func (withDefaults) InsertOpts() jobs.InsertOpts {
	return jobs.InsertOpts{Queue: "slow", Priority: 4, MaxAttempts: 2}
}

func TestInsertOptionsAndDefaults(t *testing.T) {
	drv := jobstest.New()
	c, err := jobs.NewClient(drv, jobs.WithLogger(log.Discard()), jobs.WithMaxAttempts(7))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	plain, _ := c.Insert(ctx, echo{N: 1}, nil)
	defaulted, _ := c.Insert(ctx, withDefaults{}, nil)
	overridden, _ := c.Insert(ctx, withDefaults{}, &jobs.InsertOpts{Queue: "fast", MaxAttempts: 9})
	results, err := c.InsertMany(ctx, []jobs.InsertParams{{Args: echo{N: 2}}, {Args: echo{N: 3}, Opts: &jobs.InsertOpts{Priority: 2}}})
	if err != nil || len(results) != 2 {
		t.Fatalf("InsertMany = %v, %v", results, err)
	}
	for _, tc := range []struct {
		id    int64
		queue string
		prio  int
		max   int
	}{
		{plain.ID, "default", 1, 7},
		{defaulted.ID, "slow", 4, 2},
		{overridden.ID, "fast", 4, 9},
		{results[1].ID, "default", 2, 7},
	} {
		r := row(t, drv, tc.id)
		if r.Queue != tc.queue || r.Priority != tc.prio || r.MaxAttempts != tc.max {
			t.Errorf("job %d = queue %q priority %d max %d, want %q %d %d", tc.id, r.Queue, r.Priority, r.MaxAttempts, tc.queue, tc.prio, tc.max)
		}
	}
	if r := row(t, drv, plain.ID); string(r.RawArgs) != `{"n":1}` {
		t.Fatalf("args = %s", r.RawArgs)
	}
}

func TestInsertValidation(t *testing.T) {
	c, err := jobs.NewClient(jobstest.New(), jobs.WithLogger(log.Discard()))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		args jobs.Args
		opts *jobs.InsertOpts
	}{
		{"nil args", nil, nil},
		{"queue with a space", echo{}, &jobs.InsertOpts{Queue: "a b"}},
		{"queue too long", echo{}, &jobs.InsertOpts{Queue: strings.Repeat("q", 65)}},
		{"priority too high", echo{}, &jobs.InsertOpts{Priority: 40000}},
		{"negative priority", echo{}, &jobs.InsertOpts{Priority: -1}},
		{"negative attempts", echo{}, &jobs.InsertOpts{MaxAttempts: -1}},
		{"unique key too long", echo{}, &jobs.InsertOpts{UniqueKey: strings.Repeat("k", 256)}},
		{"bad kind", badKind{}, nil},
		{"unencodable args", unencodable{}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.Insert(context.Background(), tc.args, tc.opts)
			if err == nil {
				t.Fatal("accepted")
			}
			if errs.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument: %v", errs.Code(err), err)
			}
		})
	}
}

type badKind struct{}

func (badKind) Kind() string { return "has spaces" }

type unencodable struct{ C chan int }

func (unencodable) Kind() string { return "unencodable" }

func TestNewClientValidation(t *testing.T) {
	drv := jobstest.New()
	tests := []struct {
		name string
		opts []jobs.Option
	}{
		{"no queues", []jobs.Option{jobs.WithQueues()}},
		{"queue without workers", []jobs.Option{jobs.WithQueues(jobs.Queue{Name: "a"})}},
		{"bad queue name", []jobs.Option{jobs.WithQueues(jobs.Queue{Name: "a b", Workers: 1})}},
		{"duplicate queue", []jobs.Option{jobs.WithQueues(jobs.Queue{Name: "a", Workers: 1}, jobs.Queue{Name: "a", Workers: 1})}},
		{"lease too short", []jobs.Option{jobs.WithLease(time.Millisecond)}},
		{"zero attempts", []jobs.Option{jobs.WithMaxAttempts(0)}},
		{"backoff max below base", []jobs.Option{jobs.WithRetryBackoff(time.Minute, time.Second)}},
		{"periodic too fast", []jobs.Option{jobs.WithPeriodic(jobs.Every(time.Millisecond, func() jobs.Args { return tick{} }))}},
		{"periodic without New", []jobs.Option{jobs.WithPeriodic(jobs.Periodic{Interval: time.Minute})}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := jobs.NewClient(drv, tc.opts...); err == nil {
				t.Fatal("accepted")
			}
		})
	}
	if _, err := jobs.NewClient[jobstest.Tx](nil); err == nil {
		t.Fatal("nil driver accepted")
	}
}

func TestRegistry(t *testing.T) {
	reg := jobs.NewRegistry()
	w := jobs.WorkFunc[echo](func(context.Context, *jobs.Job[echo]) error { return nil })
	if err := jobs.Register(reg, w); err != nil {
		t.Fatal(err)
	}
	if err := jobs.Register(reg, w); err == nil {
		t.Fatal("a kind was registered twice")
	}
	if err := jobs.Register(reg, jobs.WorkFunc[badKind](func(context.Context, *jobs.Job[badKind]) error { return nil })); err == nil {
		t.Fatal("an invalid kind was registered")
	}
	if err := jobs.Register[echo](nil, w); err == nil {
		t.Fatal("nil registry accepted")
	}
	if k := reg.Kinds(); len(k) != 1 || k[0] != "echo" {
		t.Fatalf("kinds = %v", k)
	}
}

type slowArgs struct{}

func (slowArgs) Kind() string { return "slow" }

type slowWorker struct{ ran *atomic.Bool }

func (w slowWorker) Work(ctx context.Context, _ *jobs.Job[slowArgs]) error {
	w.ran.Store(true)
	<-ctx.Done()
	return ctx.Err()
}
func (slowWorker) Timeout() time.Duration { return 2 * time.Second }

func TestWorkerTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		var ran atomic.Bool
		if err := jobs.Register[slowArgs](reg, slowWorker{&ran}); err != nil {
			t.Fatal(err)
		}
		c := newClient(t, drv, reg, jobs.WithJobTimeout(time.Hour))
		stop := start(t, c)
		defer stop()
		res, _ := c.Insert(context.Background(), slowArgs{}, &jobs.InsertOpts{MaxAttempts: 1})
		time.Sleep(10 * time.Second)
		synctest.Wait()
		r := row(t, drv, res.ID)
		if !ran.Load() || r.State != jobs.StateDiscarded || !strings.Contains(r.Errors[0].Error, "deadline exceeded") {
			t.Fatalf("row = %+v", r)
		}
	})
}

func TestStateFinal(t *testing.T) {
	for s, want := range map[jobs.State]bool{
		jobs.StateAvailable: false, jobs.StateRunning: false,
		jobs.StateCompleted: true, jobs.StateCancelled: true, jobs.StateDiscarded: true,
	} {
		if s.Final() != want {
			t.Errorf("%s.Final() = %v", s, !want)
		}
	}
}

func TestLongErrorsAreCut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drv := jobstest.New()
		reg := jobs.NewRegistry()
		register(t, reg, func(context.Context, *jobs.Job[echo]) error { return errors.New(strings.Repeat("é", 5000)) })
		c := newClient(t, drv, reg)
		stop := start(t, c)
		defer stop()
		res, _ := c.Insert(context.Background(), echo{}, &jobs.InsertOpts{MaxAttempts: 1})
		synctest.Wait()
		msg := row(t, drv, res.ID).Errors[0].Error
		if len(msg) > 2048 || strings.ContainsRune(msg, '�') || len(msg) < 2000 {
			t.Fatalf("stored error is %d bytes: %.20q", len(msg), msg)
		}
	})
}

func TestStateEnum(t *testing.T) {
	for _, s := range jobs.States {
		got, err := jobs.ParseState(s.String())
		if err != nil || got != s || !s.Valid() {
			t.Errorf("ParseState(%q) = %q, %v", s, got, err)
		}
	}
	for _, bad := range []string{"", "Available", "done", "running "} {
		if _, err := jobs.ParseState(bad); err == nil {
			t.Errorf("ParseState(%q) accepted", bad)
		}
		if jobs.State(bad).Valid() {
			t.Errorf("State(%q).Valid()", bad)
		}
	}
}
