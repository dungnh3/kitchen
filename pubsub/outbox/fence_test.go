package outbox_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/outbox"
)

// A publish that outlives the lease lets another relay re-claim the record.
// The first relay's late settle must not touch the record any more, and it
// must not fail the round (F9).
func TestRelayLateSettleAfterReclaimIsNoop(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result error
	}{
		{"sent", nil},
		{"failed", errorString("rejected")},
		{"dead", pubsub.Permanent(errorString("malformed"))},
		{"released", pubsub.ErrKeyPaused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				st := &memStore{}
				st.add("t", &pubsub.Message{Key: "k", Data: []byte("x")})
				pub := &funcPub{fn: func(ctx context.Context, msgs []*pubsub.Message) ([]string, error) {
					time.Sleep(2 * time.Second) // the 1s lease lapses
					// Another relay re-claims the record meanwhile.
					if recs, _ := st.Claim(context.WithoutCancel(ctx), 10, time.Minute); len(recs) != 1 {
						t.Errorf("re-claim got %d records", len(recs))
					}
					if tc.result == nil {
						return make([]string, len(msgs)), nil
					}
					return nil, tc.result
				}}
				r := newRelay(t, st, pub, outbox.WithLease(time.Second))
				// The publish deadline is lease − settle timeout; let the fake
				// publisher ignore it, like a broker that answers late.
				if _, err := r.RunOnce(context.Background()); err != nil {
					t.Fatalf("RunOnce = %v, want a lost claim to be a logged no-op", err)
				}
				rw, ok := st.get("x")
				switch {
				case !ok:
					t.Fatal("the late Sent deleted a record another relay holds")
				case rw.dead:
					t.Fatal("the late Dead marked a record another relay holds")
				case rw.claimedUntil.IsZero() || rw.lastErr != "":
					t.Fatalf("the late settle released the other relay's claim (claimed until %v, last error %q)", rw.claimedUntil, rw.lastErr)
				case rw.rec.Attempts != 2:
					t.Fatalf("attempts = %d, want 2 (two claims, nothing taken back)", rw.rec.Attempts)
				}
			})
		})
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }
