package pgx_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
	"github.com/dungnh3/kitchen/pubsub/outbox"
)

func data(recs []outbox.Record) []string {
	var out []string
	for _, r := range recs {
		out = append(out, string(r.Message.Data))
	}
	return out
}

// Claim hands out the run of consecutive due records of a key (H6): it stops
// at a record that is backed off or of another topic, and no other relay gets
// any record of the key while the run is leased.
func TestClaimRunOfKey(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	enqueue(ctx, t, pool, s, true,
		&pubsub.Message{Key: "k", Data: []byte("k1")},
		&pubsub.Message{Key: "k", Data: []byte("k2")},
		&pubsub.Message{Data: []byte("free")},
		&pubsub.Message{Key: "k", Data: []byte("k3")},
		&pubsub.Message{Key: "j", Data: []byte("j1")},
		&pubsub.Message{Key: "k", Data: []byte("k4")})
	if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return s.Enqueue(ctx, tx, "other-topic", &pubsub.Message{Key: "k", Data: []byte("k5")})
	}); err != nil {
		t.Fatal(err)
	}
	enqueue(ctx, t, pool, s, true, &pubsub.Message{Key: "k", Data: []byte("k6")})
	// Back off k4: the run of k must stop before it.
	if _, err := pool.Exec(ctx, "UPDATE "+tableOf(s)+" SET available_at = now() + interval '1 hour' WHERE payload = 'k4'"); err != nil {
		t.Fatal(err)
	}

	recs, err := s.Claim(ctx, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := data(recs), []string{"k1", "k2", "free", "k3", "j1"}; !slices.Equal(got, want) {
		t.Fatalf("claimed %v, want %v", got, want)
	}
	if again, err := s.Claim(ctx, 10, time.Minute); err != nil || len(again) != 0 {
		t.Fatalf("second claim = %v, %v; want nothing while the runs are leased", data(again), err)
	}

	// The limit cuts a run, never leaves a gap.
	if err := s.Release(ctx, []outbox.Ref{recs[0].Ref(), recs[1].Ref(), recs[3].Ref()}, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Claim(ctx, 1, time.Minute); !slices.Equal(data(got), []string{"k1"}) {
		t.Fatalf("limit 1 claimed %v, want [k1]", data(got))
	}
	if got, _ := s.Claim(ctx, 10, time.Minute); len(got) != 0 {
		t.Fatalf("claimed %v behind a leased head", data(got))
	}
}

// Release puts records back without spending the attempt and keeps the last
// error unless a new cause is given.
func TestReleaseDoesNotCountAttempt(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	enqueue(ctx, t, pool, s, true, &pubsub.Message{Key: "k", Data: []byte("k1")})
	recs, _ := s.Claim(ctx, 10, time.Minute)
	if err := s.Release(ctx, []outbox.Ref{recs[0].Ref()}, errors.New("broker down"), time.Now()); err != nil {
		t.Fatal(err)
	}
	recs, _ = s.Claim(ctx, 10, time.Minute)
	if len(recs) != 1 || recs[0].Attempts != 1 {
		t.Fatalf("after release claimed %+v, want attempts 1", recs)
	}
	if err := s.Release(ctx, []outbox.Ref{recs[0].Ref()}, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	var lastErr string
	if err := pool.QueryRow(ctx, "SELECT last_error FROM "+tableOf(s)).Scan(&lastErr); err != nil || lastErr != "broker down" {
		t.Fatalf("last_error = %q, %v", lastErr, err)
	}
}

// A key with a backlog drains in one round instead of one record per poll
// interval (H6). The poll interval is an hour and nothing wakes the relay.
func TestRelayDrainsKeyBacklogWithoutPolling(t *testing.T) {
	s, pool := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	broker := memory.New()
	sub, _ := broker.Subscribe("orders", "test")
	var msgs []*pubsub.Message
	for i := range 10 {
		msgs = append(msgs, &pubsub.Message{Key: "order-1", Data: []byte{byte('0' + i)}})
	}
	enqueue(ctx, t, pool, s, true, msgs...)
	relay, err := outbox.NewRelay(s, broker, outbox.WithPollInterval(time.Hour), outbox.WithLogger(quiet))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Go(func() { _ = relay.Run(ctx) })
	var got []byte
	rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
	defer rcancel()
	_ = sub.Receive(rctx, func(_ context.Context, d pubsub.Delivery) {
		got = append(got, d.Message().Data...)
		d.Ack()
		if len(got) == 10 {
			rcancel()
		}
	})
	cancel()
	wg.Wait()
	if string(got) != "0123456789" {
		t.Fatalf("received %q within 5s, want 0123456789 in order", got)
	}
}

// Two transactions enqueue for one key; the one with the lower id commits
// last. Enqueue serialises same-key writers, so ids follow commit order and
// two relays never hold records of the key at once (M7).
func TestEnqueueSerialisesSameKeyWriters(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()

	txA, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = txA.Rollback(ctx) }()
	if err := s.Enqueue(ctx, txA, "orders", &pubsub.Message{Key: "k", Data: []byte("A")}); err != nil {
		t.Fatal(err)
	}
	var pidB int
	pidReady := make(chan struct{})
	doneB := make(chan error, 1)
	go func() {
		doneB <- pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			if err := tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pidB); err != nil {
				close(pidReady)
				return err
			}
			close(pidReady)
			return s.Enqueue(ctx, tx, "orders", &pubsub.Message{Key: "k", Data: []byte("B")})
		})
	}()
	<-pidReady
	// Wait until B either committed or blocks on A.
	var errB error
	doneBefore, blocked := false, false
wait:
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		select {
		case errB = <-doneB:
			doneBefore = true
			break wait
		default:
		}
		if err := pool.QueryRow(ctx, "SELECT coalesce(wait_event_type = 'Lock', false) FROM pg_stat_activity WHERE pid = $1", pidB).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
	}
	if errB != nil {
		t.Fatal(errB)
	}

	r1, err := s.Claim(ctx, 10, time.Minute) // relay 1
	if err != nil {
		t.Fatal(err)
	}
	if err := txA.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if !doneBefore {
		if err := <-doneB; err != nil {
			t.Fatal(err)
		}
	}
	r2, err := s.Claim(ctx, 10, time.Minute) // relay 2, while relay 1 holds its claim
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("B blocked on A: %v; relay 1 claimed %v, relay 2 claimed %v", blocked, data(r1), data(r2))
	if len(r1) > 0 && len(r2) > 0 {
		t.Fatalf("two relays hold records of key k at once: %v and %v", data(r1), data(r2))
	}
	all := append(r1, r2...)
	if got := data(all); !slices.Equal(got, []string{"A", "B"}) || all[0].ID > all[1].ID {
		t.Fatalf("claimed %v (ids %d, %d), want A then B in id order", got, all[0].ID, all[1].ID)
	}
}

// Enqueue takes the key locks in a stable order, so two transactions that
// enqueue the same keys in opposite orders do not deadlock (M7).
func TestEnqueueKeyLocksDoNotDeadlock(t *testing.T) {
	s, pool := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errc := make(chan error, 2)
	for _, keys := range [][]string{{"a", "b", "c"}, {"c", "b", "a"}} {
		wg.Go(func() {
			for range 50 {
				var msgs []*pubsub.Message
				for _, k := range keys {
					msgs = append(msgs, &pubsub.Message{Key: k, Data: []byte(k)})
				}
				if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error { return s.Enqueue(ctx, tx, "orders", msgs...) }); err != nil {
					errc <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
}

// A publisher that hangs until its deadline must leave time to settle: the
// row backs off with last_error set, and WithMaxAttempts applies (H5).
func TestRelaySettlesAfterHungPublisher(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	enqueue(ctx, t, pool, s, true, &pubsub.Message{Key: "k", Data: []byte("x")})
	r, err := outbox.NewRelay(s, hangPub{}, outbox.WithLease(400*time.Millisecond), outbox.WithLogger(quiet),
		outbox.WithRetryBackoff(time.Millisecond, time.Millisecond), outbox.WithMaxAttempts(2))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if _, err := r.RunOnce(ctx); err != nil {
			t.Fatalf("round %d: %v", i+1, err)
		}
		time.Sleep(5 * time.Millisecond) // past the 1ms backoff
	}
	var attempts int
	var lastErr *string
	var dead *time.Time
	if err := pool.QueryRow(ctx, "SELECT attempts, last_error, dead_at FROM "+tableOf(s)).Scan(&attempts, &lastErr, &dead); err != nil {
		t.Fatal(err)
	}
	if lastErr == nil || !strings.Contains(*lastErr, "blackholed") || dead == nil {
		t.Fatalf("row: attempts=%d last_error=%v dead_at=%v; want dead after 2 attempts with the error recorded", attempts, lastErr, dead)
	}
}

// hangPub blocks until its ctx ends, then fails with a counted error.
type hangPub struct{}

func (hangPub) Publish(ctx context.Context, _ string, _ ...*pubsub.Message) ([]string, error) {
	<-ctx.Done()
	return nil, errors.New("broker: write blackholed")
}
func (hangPub) Close(context.Context) error { return nil }

func tableOf(s interface{ Schema() string }) string {
	ddl := strings.TrimPrefix(s.Schema(), "CREATE TABLE IF NOT EXISTS ")
	return ddl[:strings.Index(ddl, " ")]
}
