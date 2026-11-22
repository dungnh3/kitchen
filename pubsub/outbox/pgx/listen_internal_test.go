package pgx

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dungnh3/kitchen/log"
)

// After LISTEN succeeds again, the reconnect backoff starts over: a
// connection drop hours later must not wait as long as the failures that
// preceded the last good connection (F11).
func TestListenResetsBackoffAfterReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, err := New(&pgxpool.Pool{}, WithLogger(log.Discard()))
		if err != nil {
			t.Fatal(err)
		}
		const failsBefore = 8 // backoff grows to 100ms·2^7 = 12.8s (jittered down to half)
		var calls []time.Time
		ctx, cancel := context.WithCancel(context.Background())
		s.listenOnceFn = func(ctx context.Context, onListen func()) error {
			calls = append(calls, time.Now())
			switch n := len(calls); {
			case n <= failsBefore:
				return errors.New("connection refused")
			case n == failsBefore+1:
				onListen()
				time.Sleep(time.Hour) // a healthy LISTEN for an hour, then the connection drops
				return errors.New("connection reset")
			default:
				cancel()
				<-ctx.Done()
				return ctx.Err()
			}
		}
		if err := s.Listen(ctx); err != nil {
			t.Fatal(err)
		}
		if len(calls) != failsBefore+2 {
			t.Fatalf("listenOnce called %d times", len(calls))
		}
		wait := calls[failsBefore+1].Sub(calls[failsBefore]) - time.Hour
		if wait > 100*time.Millisecond {
			t.Fatalf("reconnect after a healthy LISTEN waited %s; want the initial backoff (<= 100ms)", wait)
		}
	})
}
