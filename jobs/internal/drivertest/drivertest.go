// Package drivertest is the conformance suite of [jobs.Store]
// implementations. Every driver runs it against a real database.
package drivertest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/jobs"
)

// Factory returns an empty store. Each call must return a store on its own
// table.
type Factory func(t *testing.T) jobs.Store

const lease = 400 * time.Millisecond

func job(kind string) jobs.NewJob {
	return jobs.NewJob{Kind: kind, Queue: "default", Args: []byte(`{"n":1}`), Metadata: []byte(`{}`), Priority: 1, MaxAttempts: 3}
}

func mustInsert(ctx context.Context, t *testing.T, s jobs.Store, in ...jobs.NewJob) []jobs.InsertResult {
	t.Helper()
	res, err := s.Insert(ctx, in)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if len(res) != len(in) {
		t.Fatalf("Insert returned %d results for %d jobs", len(res), len(in))
	}
	return res
}

func claim(ctx context.Context, t *testing.T, s jobs.Store, queue string, kinds []string, limit int) []jobs.Claimed {
	t.Helper()
	got, err := s.Claim(ctx, queue, kinds, limit, lease)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return got
}

func get(ctx context.Context, t *testing.T, s jobs.Store, id int64) *jobs.Row {
	t.Helper()
	r, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get(%d): %v", id, err)
	}
	return r
}

// eventually repeats f until it returns true; it stands in for waiting out a
// lease on a real database clock.
func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

var kinds = []string{"a", "b"}

// Run runs the suite.
func Run(ctx context.Context, t *testing.T, newStore Factory) {
	t.Run("InsertAndGet", func(t *testing.T) { insertAndGet(ctx, t, newStore(t)) })
	t.Run("GetUnknown", func(t *testing.T) {
		if _, err := newStore(t).Get(ctx, 1<<40); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("Get unknown = %v, want ErrNotFound", err)
		}
	})
	t.Run("Unique", func(t *testing.T) { unique(ctx, t, newStore(t)) })
	t.Run("ClaimFilters", func(t *testing.T) { claimFilters(ctx, t, newStore(t)) })
	t.Run("ClaimOrder", func(t *testing.T) { claimOrder(ctx, t, newStore(t)) })
	t.Run("ClaimDelayed", func(t *testing.T) { claimDelayed(ctx, t, newStore(t)) })
	t.Run("ClaimExclusive", func(t *testing.T) { claimExclusive(ctx, t, newStore(t)) })
	t.Run("SettleFenced", func(t *testing.T) { settleFenced(ctx, t, newStore(t)) })
	t.Run("SettleStates", func(t *testing.T) { settleStates(ctx, t, newStore(t)) })
	t.Run("ErrorsKeepLatest", func(t *testing.T) { errorsKeepLatest(ctx, t, newStore(t)) })
	t.Run("Extend", func(t *testing.T) { extend(ctx, t, newStore(t)) })
	t.Run("ReapExhausted", func(t *testing.T) { reapExhausted(ctx, t, newStore(t)) })
	t.Run("Cleanup", func(t *testing.T) { cleanup(ctx, t, newStore(t)) })
}

func insertAndGet(ctx context.Context, t *testing.T, s jobs.Store) {
	n := job("a")
	n.Queue, n.Priority, n.MaxAttempts = "mail", 2, 7
	n.Args = []byte(`{"to":"x@example.com","emoji":"✓"}`)
	n.Metadata = []byte(`{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}`)
	id := mustInsert(ctx, t, s, n)[0].ID
	r := get(ctx, t, s, id)
	if r.Kind != "a" || r.Queue != "mail" || r.Priority != 2 || r.MaxAttempts != 7 || r.State != jobs.StateAvailable ||
		r.Attempt != 0 || r.Claim != 0 || r.UniqueKey != "" || !r.FinalizedAt.IsZero() || len(r.Errors) != 0 {
		t.Fatalf("row = %+v", r)
	}
	if string(r.RawArgs) == "" || r.CreatedAt.IsZero() || r.ScheduledAt.IsZero() {
		t.Fatalf("row = %+v", r)
	}
	if !jsonEqual(r.RawArgs, n.Args) || !jsonEqual(r.Metadata, n.Metadata) {
		t.Fatalf("args %s metadata %s", r.RawArgs, r.Metadata)
	}
	// Several jobs in one call keep their order.
	res := mustInsert(ctx, t, s, job("a"), job("b"), job("a"))
	if res[0].ID >= res[1].ID || res[1].ID >= res[2].ID {
		t.Fatalf("ids not increasing: %+v", res)
	}
}

func unique(ctx context.Context, t *testing.T, s jobs.Store) {
	k := job("a")
	k.UniqueKey = "k1"
	first := mustInsert(ctx, t, s, k)[0]
	dup := mustInsert(ctx, t, s, k)[0]
	if first.Duplicate || !dup.Duplicate || dup.ID != first.ID {
		t.Fatalf("first %+v dup %+v", first, dup)
	}
	// Duplicates inside one call, next to a job without a key.
	other := job("b")
	k2 := job("a")
	k2.UniqueKey = "k2"
	res := mustInsert(ctx, t, s, k2, other, k2)
	if res[0].Duplicate || res[1].Duplicate || !res[2].Duplicate || res[2].ID != res[0].ID {
		t.Fatalf("batch = %+v", res)
	}
	// The key is held while the job is pending, and released when it ends.
	got := claim(ctx, t, s, "default", kinds, 1)
	if len(got) != 1 || got[0].ID != first.ID {
		t.Fatalf("claimed %+v", got)
	}
	if !mustInsert(ctx, t, s, k)[0].Duplicate {
		t.Fatal("key released while running")
	}
	if err := s.Settle(ctx, got[0].Ref(), jobs.Settlement{State: jobs.StateCompleted}); err != nil {
		t.Fatal(err)
	}
	again := mustInsert(ctx, t, s, k)[0]
	if again.Duplicate || again.ID == first.ID {
		t.Fatalf("key not released after completion: %+v", again)
	}
	// KeepUnique holds the key after the job ended.
	keep := job("a")
	keep.UniqueKey, keep.KeepUnique = "k3", true
	kid := mustInsert(ctx, t, s, keep)[0].ID
	for _, c := range claim(ctx, t, s, "default", kinds, 100) {
		if c.ID == kid {
			if err := s.Settle(ctx, c.Ref(), jobs.Settlement{State: jobs.StateCompleted}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if d := mustInsert(ctx, t, s, keep)[0]; !d.Duplicate || d.ID != kid {
		t.Fatalf("kept key not held: %+v", d)
	}
}

func claimFilters(ctx context.Context, t *testing.T, s jobs.Store) {
	a, b, c := job("a"), job("b"), job("c")
	other := job("a")
	other.Queue = "other"
	mustInsert(ctx, t, s, a, b, c, other)
	got := claim(ctx, t, s, "default", []string{"a", "b"}, 10)
	if len(got) != 2 {
		t.Fatalf("claimed %d, want 2 (kind c and queue other are excluded)", len(got))
	}
	for _, g := range got {
		if g.Queue != "default" || g.Kind == "c" || g.State != jobs.StateRunning || g.Attempt != 1 || g.Claim != 1 {
			t.Fatalf("claimed %+v", g.Row)
		}
	}
	if more := claim(ctx, t, s, "default", []string{"a", "b"}, 10); len(more) != 0 {
		t.Fatalf("claimed running jobs again: %+v", more)
	}
	if len(claim(ctx, t, s, "default", nil, 10)) != 0 {
		t.Fatal("claimed with no kinds")
	}
	if len(claim(ctx, t, s, "other", []string{"a"}, 10)) != 1 {
		t.Fatal("other queue not claimable")
	}
}

func claimOrder(ctx context.Context, t *testing.T, s jobs.Store) {
	mk := func(prio int, delay time.Duration) int64 {
		n := job("a")
		n.Priority, n.Delay = prio, delay
		return mustInsert(ctx, t, s, n)[0].ID
	}
	low1 := mk(5, 0)
	low2 := mk(5, 0)
	high := mk(1, 0)
	mid := mk(3, 0)
	want := []int64{high, mid, low1, low2}
	var got []int64
	for _, c := range claim(ctx, t, s, "default", kinds, 10) {
		got = append(got, c.ID)
	}
	slices.Sort(want[2:]) // equal priority and time: ID order, and the two inserts may share a timestamp
	if !slices.Equal(got, want) {
		t.Fatalf("claim order = %v, want %v", got, want)
	}
	// limit
	for range 3 {
		mk(1, 0)
	}
	if n := len(claim(ctx, t, s, "default", kinds, 2)); n != 2 {
		t.Fatalf("limit 2 claimed %d", n)
	}
}

func claimDelayed(ctx context.Context, t *testing.T, s jobs.Store) {
	n := job("a")
	n.Delay = 700 * time.Millisecond
	id := mustInsert(ctx, t, s, n)[0].ID
	if got := claim(ctx, t, s, "default", kinds, 1); len(got) != 0 {
		t.Fatalf("claimed a job before it was due: %+v", got)
	}
	var lag time.Duration
	eventually(t, "the job to become due", func() bool {
		got := claim(ctx, t, s, "default", kinds, 1)
		if len(got) == 1 {
			lag = got[0].Lag
			if got[0].ID != id {
				t.Fatalf("claimed %d, want %d", got[0].ID, id)
			}
		}
		return len(got) == 1
	})
	if lag < 0 || lag > 5*time.Second {
		t.Fatalf("lag = %v", lag)
	}
}

func claimExclusive(ctx context.Context, t *testing.T, s jobs.Store) {
	const jobsN, claimers = 60, 8
	in := make([]jobs.NewJob, jobsN)
	for i := range in {
		in[i] = job("a")
	}
	mustInsert(ctx, t, s, in...)
	var (
		mu   sync.Mutex
		seen = map[int64]int{}
		wg   sync.WaitGroup
	)
	for range claimers {
		wg.Go(func() {
			for {
				got, err := s.Claim(ctx, "default", kinds, 5, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, g := range got {
					seen[g.ID]++
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if len(seen) != jobsN {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), jobsN)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %d claimed %d times", id, n)
		}
	}
}

func settleFenced(ctx context.Context, t *testing.T, s jobs.Store) {
	id := mustInsert(ctx, t, s, job("a"))[0].ID
	first := claim(ctx, t, s, "default", kinds, 1)
	if len(first) != 1 {
		t.Fatal("nothing claimed")
	}
	// The lease lapses and a second worker claims the job.
	var second jobs.Claimed
	eventually(t, "the lease to lapse", func() bool {
		got := claim(ctx, t, s, "default", kinds, 1)
		if len(got) == 1 {
			second = got[0]
		}
		return len(got) == 1
	})
	if second.ID != id || second.Claim != first[0].Claim+1 || second.Attempt != 2 {
		t.Fatalf("second claim = %+v, first = %+v", second.Row, first[0].Row)
	}
	// The first worker's late result changes nothing.
	err := s.Settle(ctx, first[0].Ref(), jobs.Settlement{State: jobs.StateCompleted})
	if !errors.Is(err, jobs.ErrClaimLost) {
		t.Fatalf("stale settle = %v, want ErrClaimLost", err)
	}
	if r := get(ctx, t, s, id); r.State != jobs.StateRunning || r.Claim != second.Claim {
		t.Fatalf("stale settle changed the row: %+v", r)
	}
	if err := s.Settle(ctx, second.Ref(), jobs.Settlement{State: jobs.StateCompleted}); err != nil {
		t.Fatal(err)
	}
	if r := get(ctx, t, s, id); r.State != jobs.StateCompleted || r.FinalizedAt.IsZero() {
		t.Fatalf("row = %+v", r)
	}
	// Settling twice, and settling a job that is not running, is a lost claim.
	if err := s.Settle(ctx, second.Ref(), jobs.Settlement{State: jobs.StateCompleted}); !errors.Is(err, jobs.ErrClaimLost) {
		t.Fatalf("second settle = %v", err)
	}
	if err := s.Settle(ctx, jobs.Ref{ID: 1 << 40, Claim: 1}, jobs.Settlement{State: jobs.StateCompleted}); !errors.Is(err, jobs.ErrClaimLost) {
		t.Fatalf("settle of unknown job = %v", err)
	}
}

func settleStates(ctx context.Context, t *testing.T, s jobs.Store) {
	at := time.Now().UTC()
	tests := []struct {
		name     string
		settle   jobs.Settlement
		state    jobs.State
		attempt  int
		wantErrs int
		final    bool
	}{
		{"completed", jobs.Settlement{State: jobs.StateCompleted}, jobs.StateCompleted, 1, 0, true},
		{"cancelled", jobs.Settlement{State: jobs.StateCancelled, Error: &jobs.AttemptError{At: at, Attempt: 1, Error: "stop"}}, jobs.StateCancelled, 1, 1, true},
		{"discarded", jobs.Settlement{State: jobs.StateDiscarded, Error: &jobs.AttemptError{At: at, Attempt: 1, Error: "boom"}}, jobs.StateDiscarded, 1, 1, true},
		{"retry", jobs.Settlement{State: jobs.StateAvailable, Delay: time.Hour, Error: &jobs.AttemptError{At: at, Attempt: 1, Error: "later"}}, jobs.StateAvailable, 1, 1, false},
		{"snooze gives the attempt back", jobs.Settlement{State: jobs.StateAvailable, Delay: time.Hour, GiveBackAttempt: true}, jobs.StateAvailable, 0, 0, false},
		{"release now", jobs.Settlement{State: jobs.StateAvailable, GiveBackAttempt: true}, jobs.StateAvailable, 0, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// A queue per case keeps the cases independent.
			n := job("a")
			n.Queue = "q-" + fmt.Sprint(time.Now().UnixNano())
			id := mustInsert(ctx, t, s, n)[0].ID
			c := claim(ctx, t, s, n.Queue, kinds, 1)
			if len(c) != 1 || c[0].ID != id {
				t.Fatalf("claimed %+v", c)
			}
			if err := s.Settle(ctx, c[0].Ref(), tc.settle); err != nil {
				t.Fatal(err)
			}
			r := get(ctx, t, s, id)
			if r.State != tc.state || r.Attempt != tc.attempt || len(r.Errors) != tc.wantErrs || r.FinalizedAt.IsZero() == tc.final {
				t.Fatalf("row = %+v", r)
			}
			if tc.wantErrs == 1 && (r.Errors[0].Error == "" || r.Errors[0].Attempt != 1) {
				t.Fatalf("errors = %+v", r.Errors)
			}
			if tc.state == jobs.StateAvailable && tc.settle.Delay == 0 {
				if len(claim(ctx, t, s, n.Queue, kinds, 1)) != 1 {
					t.Fatal("released job is not claimable at once")
				}
			}
			if tc.state == jobs.StateAvailable && tc.settle.Delay > 0 {
				if len(claim(ctx, t, s, n.Queue, kinds, 1)) != 0 {
					t.Fatal("delayed job claimable at once")
				}
			}
		})
	}
}

func errorsKeepLatest(ctx context.Context, t *testing.T, s jobs.Store) {
	n := job("a")
	n.MaxAttempts = 100
	id := mustInsert(ctx, t, s, n)[0].ID
	for i := 1; i <= 13; i++ {
		c := claim(ctx, t, s, "default", kinds, 1)
		if len(c) != 1 {
			t.Fatalf("attempt %d: nothing claimed", i)
		}
		err := s.Settle(ctx, c[0].Ref(), jobs.Settlement{
			State: jobs.StateAvailable,
			Error: &jobs.AttemptError{At: time.Now().UTC(), Attempt: i, Error: fmt.Sprintf("failure %d ✓", i)},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	r := get(ctx, t, s, id)
	if len(r.Errors) != 10 || r.Errors[0].Attempt != 4 || r.Errors[9].Attempt != 13 || r.Errors[9].Error != "failure 13 ✓" {
		t.Fatalf("errors = %+v", r.Errors)
	}
}

func extend(ctx context.Context, t *testing.T, s jobs.Store) {
	id := mustInsert(ctx, t, s, job("a"))[0].ID
	c := claim(ctx, t, s, "default", kinds, 1)[0]
	// Renewing keeps the job from being claimed again past its first lease.
	deadline := time.Now().Add(3 * lease)
	for time.Now().Before(deadline) {
		lost, err := s.Extend(ctx, []jobs.Ref{c.Ref()}, lease)
		if err != nil || len(lost) != 0 {
			t.Fatalf("Extend = %v, %v", lost, err)
		}
		if got := claim(ctx, t, s, "default", kinds, 1); len(got) != 0 {
			t.Fatalf("a renewed job was claimed again: %+v", got)
		}
		time.Sleep(lease / 4)
	}
	// Once the lease lapses and another worker claims the job, the old
	// claim is reported lost and the new one is not.
	var second jobs.Claimed
	eventually(t, "the lease to lapse", func() bool {
		got := claim(ctx, t, s, "default", kinds, 1)
		if len(got) == 1 {
			second = got[0]
		}
		return len(got) == 1
	})
	lost, err := s.Extend(ctx, []jobs.Ref{c.Ref(), second.Ref(), {ID: 1 << 40, Claim: 1}}, lease)
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 2 || !slices.Contains(lost, c.Ref()) || slices.Contains(lost, second.Ref()) {
		t.Fatalf("lost = %+v", lost)
	}
	_ = id
	if lost, err := s.Extend(ctx, nil, lease); err != nil || len(lost) != 0 {
		t.Fatalf("Extend(nil) = %v, %v", lost, err)
	}
}

func reapExhausted(ctx context.Context, t *testing.T, s jobs.Store) {
	n := job("a")
	n.MaxAttempts = 1
	id := mustInsert(ctx, t, s, n)[0].ID
	if len(claim(ctx, t, s, "default", kinds, 1)) != 1 {
		t.Fatal("nothing claimed")
	}
	if got, err := s.Reap(ctx); err != nil || got != 0 {
		t.Fatalf("Reap with a live lease = %d, %v", got, err)
	}
	eventually(t, "the lease to lapse", func() bool {
		got, err := s.Reap(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return got == 1
	})
	r := get(ctx, t, s, id)
	if r.State != jobs.StateDiscarded || r.FinalizedAt.IsZero() || len(r.Errors) != 1 {
		t.Fatalf("row = %+v", r)
	}
	// A lapsed job with attempts left is claimed again, not reaped.
	n.MaxAttempts = 2
	mustInsert(ctx, t, s, n)
	if len(claim(ctx, t, s, "default", kinds, 1)) != 1 {
		t.Fatal("nothing claimed")
	}
	eventually(t, "the lease to lapse", func() bool { return len(claim(ctx, t, s, "default", kinds, 1)) == 1 })
	// The second attempt was the last: once its lease lapses nothing claims it.
	time.Sleep(2 * lease)
	if got := claim(ctx, t, s, "default", kinds, 1); len(got) != 0 {
		t.Fatalf("claimed a job without attempts left: %+v", got)
	}
}

func cleanup(ctx context.Context, t *testing.T, s jobs.Store) {
	finish := func(state jobs.State) int64 {
		id := mustInsert(ctx, t, s, job("a"))[0].ID
		var c jobs.Claimed
		for _, g := range claim(ctx, t, s, "default", kinds, 100) {
			if g.ID == id {
				c = g
			}
		}
		if c.ID == 0 {
			t.Fatal("not claimed")
		}
		if err := s.Settle(ctx, c.Ref(), jobs.Settlement{State: state}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	done1, done2 := finish(jobs.StateCompleted), finish(jobs.StateCompleted)
	cancelled, discarded := finish(jobs.StateCancelled), finish(jobs.StateDiscarded)
	pending := mustInsert(ctx, t, s, job("a"))[0].ID

	// Nothing is old enough.
	if n, err := s.Cleanup(ctx, jobs.CleanupParams{Completed: time.Hour, Discarded: time.Hour, Limit: 100}); err != nil || n != 0 {
		t.Fatalf("Cleanup(1h) = %d, %v", n, err)
	}
	// The two retentions apply to their own states.
	time.Sleep(50 * time.Millisecond)
	if n, err := s.Cleanup(ctx, jobs.CleanupParams{Completed: 0, Discarded: time.Hour, Limit: 100}); err != nil || n != 2 {
		t.Fatalf("Cleanup(completed) = %d, %v", n, err)
	}
	for _, id := range []int64{done1, done2} {
		if _, err := s.Get(ctx, id); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("job %d survived: %v", id, err)
		}
	}
	// Limit bounds the batch.
	if n, err := s.Cleanup(ctx, jobs.CleanupParams{Completed: 0, Discarded: 0, Limit: 1}); err != nil || n != 1 {
		t.Fatalf("Cleanup(limit 1) = %d, %v", n, err)
	}
	if n, err := s.Cleanup(ctx, jobs.CleanupParams{Completed: 0, Discarded: 0, Limit: 10}); err != nil || n != 1 {
		t.Fatalf("Cleanup(rest) = %d, %v", n, err)
	}
	for _, id := range []int64{cancelled, discarded} {
		if _, err := s.Get(ctx, id); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("job %d survived: %v", id, err)
		}
	}
	if r := get(ctx, t, s, pending); r.State != jobs.StateAvailable {
		t.Fatalf("an unfinished job was deleted or changed: %+v", r)
	}
}
