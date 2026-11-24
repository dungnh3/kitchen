package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/outbox"
)

// funcPub publishes through fn and records every call.
type funcPub struct {
	mu    sync.Mutex
	fn    func(ctx context.Context, msgs []*pubsub.Message) ([]string, error)
	calls [][]*pubsub.Message
}

func (p *funcPub) Publish(ctx context.Context, _ string, msgs ...*pubsub.Message) ([]string, error) {
	p.mu.Lock()
	p.calls = append(p.calls, msgs)
	fn := p.fn
	p.mu.Unlock()
	if fn == nil {
		return make([]string, len(msgs)), nil
	}
	return fn(ctx, msgs)
}
func (p *funcPub) Close(context.Context) error { return nil }

func (p *funcPub) setFn(fn func(ctx context.Context, msgs []*pubsub.Message) ([]string, error)) {
	p.mu.Lock()
	p.fn = fn
	p.mu.Unlock()
}

func (p *funcPub) callData() [][]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out [][]string
	for _, c := range p.calls {
		var d []string
		for _, m := range c {
			d = append(d, string(m.Data))
		}
		out = append(out, d)
	}
	return out
}

func newRelay(t *testing.T, st outbox.Store, pub pubsub.Publisher, opts ...outbox.RelayOption) *outbox.Relay {
	t.Helper()
	r, err := outbox.NewRelay(st, pub, append([]outbox.RelayOption{outbox.WithLogger(log.Discard())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// A publisher that hangs until its deadline must leave the relay enough time
// to settle: the failure is recorded, the record backs off, and
// WithMaxAttempts still applies.
func TestRelaySettlesAfterHungPublish(t *testing.T) {
	for _, tc := range []struct {
		name     string
		hangErr  func(ctx context.Context) error
		wantDead bool
	}{
		// The broker gives up with its own error once the deadline passes:
		// a counted failure, so maxAttempts (2) marks the record dead.
		{"counted error", func(context.Context) error { return errors.New("broker: write blackholed") }, true},
		// The publish is cut by the relay's deadline: transient, so it backs
		// off and records the error but never dies.
		{"deadline", func(ctx context.Context) error { return ctx.Err() }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				st := &memStore{}
				st.add("t", &pubsub.Message{Key: "k", Data: []byte("x")})
				pub := &funcPub{fn: func(ctx context.Context, _ []*pubsub.Message) ([]string, error) {
					<-ctx.Done()
					return nil, tc.hangErr(ctx)
				}}
				r := newRelay(t, st, pub, outbox.WithLease(300*time.Millisecond),
					outbox.WithRetryBackoff(time.Hour, time.Hour), outbox.WithMaxAttempts(2))
				ctx := context.Background()
				for round := range 3 {
					start := time.Now()
					if _, err := r.RunOnce(ctx); err != nil {
						t.Fatalf("round %d: %v", round+1, err)
					}
					if took := time.Since(start); took > 300*time.Millisecond {
						t.Fatalf("round %d took %s, longer than the lease", round+1, took)
					}
					rw, _ := st.get("x")
					if rw.lastErr == "" {
						t.Fatalf("round %d: last error not recorded", round+1)
					}
					if !rw.dead && time.Until(rw.availableAt) < 30*time.Minute {
						t.Fatalf("round %d: not backed off (available in %s)", round+1, time.Until(rw.availableAt))
					}
					time.Sleep(2 * time.Hour) // past the backoff
				}
				rw, _ := st.get("x")
				if rw.dead != tc.wantDead {
					t.Fatalf("dead = %v, want %v (attempts %d, last error %q)", rw.dead, tc.wantDead, rw.rec.Attempts, rw.lastErr)
				}
			})
		})
	}
}

// A run of one key is published in one call; when a record fails, the later
// records of the key are neither marked sent nor charged an attempt.
func TestRelayRunOfKeyStopsAtFirstFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := &memStore{}
		for _, d := range []string{"k1", "k2", "k3", "k4"} {
			st.add("t", &pubsub.Message{Key: "k", Data: []byte(d)})
		}
		st.add("t", &pubsub.Message{Data: []byte("free")})
		// k2 fails; the broker (an SQS-like one that does not pause the key)
		// still accepts k3 and k4.
		pub := &funcPub{fn: func(_ context.Context, msgs []*pubsub.Message) ([]string, error) {
			ids := make([]string, len(msgs))
			failed := map[int]error{}
			for i, m := range msgs {
				if string(m.Data) == "k2" {
					failed[i] = errors.New("rejected")
				}
			}
			if len(failed) > 0 {
				return ids, &pubsub.PublishError{IDs: ids, Failed: failed}
			}
			return ids, nil
		}}
		r := newRelay(t, st, pub, outbox.WithRetryBackoff(time.Minute, time.Minute))
		ctx := context.Background()
		if _, err := r.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if got := pub.callData(); !slices.Equal(got[0], []string{"k1", "k2", "k3", "k4", "free"}) || len(got) != 1 {
			t.Fatalf("publish calls = %v, want one call with the whole run in order", got)
		}
		if _, ok := st.get("k1"); ok {
			t.Fatal("k1 not marked sent")
		}
		if _, ok := st.get("free"); ok {
			t.Fatal("free not marked sent")
		}
		k2, _ := st.get("k2")
		if k2.rec.Attempts != 1 || k2.lastErr == "" {
			t.Fatalf("k2 = attempts %d, last error %q; want a counted failure", k2.rec.Attempts, k2.lastErr)
		}
		for _, d := range []string{"k3", "k4"} {
			rw, ok := st.get(d)
			if !ok {
				t.Fatalf("%s marked sent although k2 before it failed", d)
			}
			if rw.rec.Attempts != 0 || !rw.claimedUntil.IsZero() {
				t.Fatalf("%s = attempts %d, claimed until %v; want released without an attempt", d, rw.rec.Attempts, rw.claimedUntil)
			}
		}

		pub.setFn(nil) // the broker recovers
		time.Sleep(2 * time.Minute)
		if _, err := r.RunOnce(ctx); err != nil {
			t.Fatal(err)
		}
		if got := pub.callData(); len(got) != 2 || !slices.Equal(got[1], []string{"k2", "k3", "k4"}) {
			t.Fatalf("publish calls = %v, want the rest of the run in order", got)
		}
		if st.len() != 0 {
			t.Fatalf("rows left: %d", st.len())
		}
	})
}

// Run claims again at once after a round that made progress, so a store that
// hands out one record per key per claim drains a key without waiting a poll
// interval per record.
func TestRelayDrainsKeyWithoutWaitingForPoll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st := &memStore{maxRun: 1}
		for i := range 10 {
			st.add("t", &pubsub.Message{Key: "k", Data: fmt.Appendf(nil, "%d", i)})
		}
		pub := &funcPub{}
		r := newRelay(t, st, pub, outbox.WithPollInterval(time.Hour))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error)
		start := time.Now()
		go func() { done <- r.Run(ctx) }()
		synctest.Wait() // Run is idle: blocked on its poll timer
		elapsed := time.Since(start)
		left := st.len()
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if left != 0 || elapsed != 0 {
			t.Fatalf("%d records left after %s; want the key drained without waiting for the poll interval", left, elapsed)
		}
	})
}

// Transient broker failures back off without counting toward
// WithMaxAttempts; other failures count.
func TestRelayTransientFailuresDoNotCount(t *testing.T) {
	unavailable := errs.Wrap(errors.New("broker down"), errs.ErrDependencyUnavailable)
	for _, tc := range []struct {
		name     string
		err      error
		entry    bool // report err as a *PublishError entry instead of the call's error
		wantDead bool
	}{
		{"unavailable", unavailable, false, false},
		{"unavailable entry", unavailable, true, false},
		{"deadline", fmt.Errorf("publish: %w", context.DeadlineExceeded), false, false},
		{"canceled entry", context.Canceled, true, false},
		{"grpc unavailable entry", status.Error(codes.Unavailable, "try again"), true, false},
		{"key paused entry", pubsub.ErrKeyPaused, true, false},
		{"plain error", errors.New("boom"), false, true},
		{"plain entry", errors.New("boom"), true, true},
		// A specific cause under the UNAVAILABLE label (what pubsub.Telemetry
		// does to a broker's InvalidArgument) is not transient.
		{"invalid argument labelled unavailable", errs.Wrap(status.Error(codes.InvalidArgument, "too large"), errs.ErrDependencyUnavailable), false, true},
		{"permanent", pubsub.Permanent(unavailable), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				st := &memStore{}
				st.add("t", &pubsub.Message{Key: "k", Data: []byte("x")})
				st.add("t", &pubsub.Message{Data: []byte("ok")})
				pub := &funcPub{fn: func(_ context.Context, msgs []*pubsub.Message) ([]string, error) {
					ids := make([]string, len(msgs))
					if !tc.entry {
						return nil, tc.err
					}
					failed := map[int]error{}
					for i, m := range msgs {
						if string(m.Data) == "x" {
							failed[i] = tc.err
						}
					}
					return ids, &pubsub.PublishError{IDs: ids, Failed: failed}
				}}
				r := newRelay(t, st, pub, outbox.WithMaxAttempts(2), outbox.WithRetryBackoff(time.Second, time.Second))
				for range 4 {
					if _, err := r.RunOnce(context.Background()); err != nil {
						t.Fatal(err)
					}
					time.Sleep(time.Minute)
				}
				rw, ok := st.get("x")
				if !ok {
					t.Fatal("record x vanished")
				}
				if rw.dead != tc.wantDead {
					t.Fatalf("dead = %v, want %v (attempts %d)", rw.dead, tc.wantDead, rw.rec.Attempts)
				}
				if !tc.wantDead && (rw.rec.Attempts != 0 || rw.lastErr == "") {
					t.Fatalf("transient failure: attempts %d, last error %q; want 0 and the error recorded", rw.rec.Attempts, rw.lastErr)
				}
			})
		})
	}
}

// Every relayed message carries outbox-id (the record ID) unless the caller
// set it, and keeps the caller's other attributes.
func TestRelaySetsOutboxIDAttribute(t *testing.T) {
	st := &memStore{}
	st.add("t", &pubsub.Message{Data: []byte("plain")})
	st.add("t", &pubsub.Message{Data: []byte("attrs"), Attributes: map[string]string{"a": "1"}})
	st.add("t", &pubsub.Message{Data: []byte("own"), Attributes: map[string]string{"outbox-id": "caller"}})
	pub := &funcPub{}
	r := newRelay(t, st, pub)
	if _, err := r.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.calls) != 1 || len(pub.calls[0]) != 3 {
		t.Fatalf("calls = %v", pub.calls)
	}
	var got []string
	for _, m := range pub.calls[0] {
		got = append(got, string(m.Data)+"="+m.Attributes["outbox-id"])
	}
	if want := []string{"plain=1", "attrs=2", "own=caller"}; !slices.Equal(got, want) {
		t.Fatalf("outbox-id attributes = %v, want %v", got, want)
	}
	if pub.calls[0][1].Attributes["a"] != "1" {
		t.Fatal("caller attributes lost")
	}
	if !strings.EqualFold(outbox.IDAttribute, "outbox-id") {
		t.Fatalf("IDAttribute = %q", outbox.IDAttribute)
	}
}
