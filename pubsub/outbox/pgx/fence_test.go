package pgx_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/outbox"
)

// Relay 1's lease lapses and relay 2 re-claims the row. A late settle by
// relay 1 must leave relay 2's claim alone: no delete, no release (which
// would let a third relay publish the row concurrently), no dead mark, and
// no attempt taken back.
func TestStaleSettleIsFenced(t *testing.T) {
	for _, tc := range []struct {
		name   string
		settle func(ctx context.Context, s outbox.Store, stale outbox.Record) error
	}{
		{"sent", func(ctx context.Context, s outbox.Store, r outbox.Record) error {
			return s.Sent(ctx, []outbox.Ref{r.Ref()})
		}},
		{"failed", func(ctx context.Context, s outbox.Store, r outbox.Record) error {
			return s.Failed(ctx, r.Ref(), errors.New("late"), time.Now())
		}},
		{"released", func(ctx context.Context, s outbox.Store, r outbox.Record) error {
			return s.Release(ctx, []outbox.Ref{r.Ref()}, errors.New("late"), time.Now())
		}},
		{"dead", func(ctx context.Context, s outbox.Store, r outbox.Record) error {
			return s.Dead(ctx, r.Ref(), errors.New("late"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, pool := newStore(t)
			ctx := context.Background()
			enqueue(ctx, t, pool, s, true, &pubsub.Message{Key: "k", Data: []byte("x")})
			r1, err := s.Claim(ctx, 10, 50*time.Millisecond)
			if err != nil || len(r1) != 1 {
				t.Fatalf("relay 1 claim = %v, %v", r1, err)
			}
			var r2 []outbox.Record
			for deadline := time.Now().Add(5 * time.Second); len(r2) == 0 && time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
				if r2, err = s.Claim(ctx, 10, time.Minute); err != nil {
					t.Fatal(err)
				}
			}
			if len(r2) != 1 {
				t.Fatal("relay 2 never re-claimed the row")
			}

			err = tc.settle(ctx, s, r1[0])
			t.Logf("late %s: %v", tc.name, err)
			if lost, ok := errors.AsType[*outbox.ClaimLostError](err); !ok || len(lost.IDs) != 1 || lost.IDs[0] != r1[0].ID {
				t.Fatalf("late settle = %v, want a ClaimLostError for record %d", err, r1[0].ID)
			}
			var attempts int
			var lastErr *string
			var dead, claimedUntil *time.Time
			if err := pool.QueryRow(ctx, "SELECT attempts, last_error, dead_at, claimed_until FROM "+tableOf(s)).
				Scan(&attempts, &lastErr, &dead, &claimedUntil); err != nil {
				t.Fatalf("row of relay 2: %v (deleted by the late settle?)", err)
			}
			if dead != nil || lastErr != nil || claimedUntil == nil || attempts != 2 {
				t.Fatalf("late settle changed relay 2's row: attempts=%d last_error=%v dead_at=%v claimed_until=%v", attempts, lastErr, dead, claimedUntil)
			}
			if again, _ := s.Claim(ctx, 10, time.Minute); len(again) != 0 {
				t.Fatal("a third relay could claim the row while relay 2 holds it")
			}
		})
	}
}

// Schema adds claim_token in place to an existing table that lacks it,
// keeping its rows claimable and settleable.
func TestSchemaUpgradesOldTable(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	tbl := tableOf(s)
	if _, err := pool.Exec(ctx, "DROP TABLE "+tbl+`;
CREATE TABLE `+tbl+` (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic         text        NOT NULL,
    ordering_key  text        NOT NULL DEFAULT '',
    payload       bytea       NOT NULL,
    attributes    jsonb       NOT NULL DEFAULT '{}',
    attempts      integer     NOT NULL DEFAULT 0,
    available_at  timestamptz NOT NULL DEFAULT now(),
    claimed_until timestamptz,
    last_error    text,
    dead_at       timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now()
);
INSERT INTO `+tbl+` (topic, ordering_key, payload) VALUES ('orders', 'k', 'old');`); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTable(ctx); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	recs, err := s.Claim(ctx, 10, time.Minute)
	if err != nil || len(recs) != 1 || recs[0].Claim != 1 {
		t.Fatalf("claim after upgrade = %+v, %v", recs, err)
	}
	if err := s.Sent(ctx, []outbox.Ref{recs[0].Ref()}); err != nil {
		t.Fatal(err)
	}
}
