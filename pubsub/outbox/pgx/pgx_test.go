package pgx_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
	"github.com/dungnh3/kitchen/pubsub/outbox"
	outboxpgx "github.com/dungnh3/kitchen/pubsub/outbox/pgx"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

var quiet = log.Discard()

func TestTableNameValidation(t *testing.T) {
	for _, name := range []string{"x; DROP TABLE users", "a-b", "1abc", `"q"`, "a.b.c", ""} {
		if _, err := outboxpgx.New(&pgxpool.Pool{}, outboxpgx.WithTable(name)); err == nil {
			t.Errorf("table %q accepted", name)
		}
	}
	s, err := outboxpgx.New(&pgxpool.Pool{}, outboxpgx.WithTable("app.events_outbox"))
	if err != nil {
		t.Fatal(err)
	}
	if want := `CREATE TABLE IF NOT EXISTS "app"."events_outbox"`; s.Schema()[:len(want)] != want {
		t.Fatalf("schema = %s", s.Schema())
	}
}

// newStore connects to KITCHEN_TEST_PG_DSN and creates a fresh table.
func newStore(t *testing.T) (*outboxpgx.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("KITCHEN_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("KITCHEN_TEST_PG_DSN not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	table := fmt.Sprintf("outbox_test_%d", time.Now().UnixNano())
	s, err := outboxpgx.New(pool, outboxpgx.WithTable(table), outboxpgx.WithLogger(quiet),
		outboxpgx.WithPropagator(propagation.TraceContext{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTable(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE "+pgx.Identifier{table}.Sanitize()) })
	return s, pool
}

func enqueue(ctx context.Context, t *testing.T, pool *pgxpool.Pool, s *outboxpgx.Store, commit bool, msgs ...*pubsub.Message) {
	t.Helper()
	errRollback := errors.New("rollback")
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if err := s.Enqueue(ctx, tx, "orders", msgs...); err != nil {
			return err
		}
		if !commit {
			return errRollback
		}
		return nil
	})
	if err != nil && !errors.Is(err, errRollback) {
		t.Fatal(err)
	}
}

func TestClaimLeasesKeyRun(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	enqueue(ctx, t, pool, s, false, &pubsub.Message{Data: []byte("rolled back")})
	enqueue(ctx, t, pool, s, true,
		&pubsub.Message{Key: "k", Data: []byte("k1")},
		&pubsub.Message{Key: "k", Data: []byte("k2")},
		&pubsub.Message{Data: []byte("free")})

	recs, err := s.Claim(ctx, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range recs {
		got = append(got, string(r.Message.Data))
	}
	if !slices.Equal(got, []string{"k1", "k2", "free"}) {
		t.Fatalf("claimed %v, want [k1 k2 free]", got)
	}
	// Leased rows are not claimable by another relay.
	if again, _ := s.Claim(ctx, 10, time.Minute); len(again) != 0 {
		t.Fatalf("second claim got %d records", len(again))
	}
	// k1 fails (counted), k2 is released behind it, free is sent.
	if err := s.Failed(ctx, recs[0].Ref(), errors.New("boom"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, []outbox.Ref{recs[1].Ref()}, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := s.Sent(ctx, []outbox.Ref{recs[2].Ref()}); err != nil {
		t.Fatal(err)
	}
	recs, _ = s.Claim(ctx, 10, time.Minute)
	if len(recs) != 2 || string(recs[0].Message.Data) != "k1" || recs[0].Attempts != 2 ||
		string(recs[1].Message.Data) != "k2" || recs[1].Attempts != 1 {
		t.Fatalf("after failure claimed %+v", recs)
	}
	_ = s.Sent(ctx, []outbox.Ref{recs[0].Ref(), recs[1].Ref()})
	if recs, _ = s.Claim(ctx, 10, time.Minute); len(recs) != 0 {
		t.Fatalf("claimed %+v from an empty outbox", recs)
	}
}

func TestDeadRowReleasesKey(t *testing.T) {
	s, pool := newStore(t)
	ctx := context.Background()
	enqueue(ctx, t, pool, s, true,
		&pubsub.Message{Key: "k", Data: []byte("k1")},
		&pubsub.Message{Key: "k", Data: []byte("k2")})
	recs, err := s.Claim(ctx, 10, time.Minute)
	if err != nil || len(recs) != 2 {
		t.Fatalf("claim = %v, %v", recs, err)
	}
	// 1000+ bytes ending in a multi-byte rune: the stored error must stay valid UTF-8.
	cause := errors.New(strings.Repeat("x", 999) + "ế")
	if err := s.Dead(ctx, recs[0].Ref(), cause); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, []outbox.Ref{recs[1].Ref()}, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	recs, _ = s.Claim(ctx, 10, time.Minute)
	if len(recs) != 1 || string(recs[0].Message.Data) != "k2" {
		t.Fatalf("after dead head claimed %+v", recs)
	}
}

func TestRelayEndToEndWithListen(t *testing.T) {
	s, pool := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker := memory.New()
	sub, _ := broker.Subscribe("orders", "test")
	relay, err := outbox.NewRelay(s, broker, outbox.WithPollInterval(time.Hour), outbox.WithLogger(quiet))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Go(func() { _ = s.Listen(ctx) })
	wg.Go(func() { _ = relay.Run(ctx) })

	tp := sdktrace.NewTracerProvider()
	tctx, span := tp.Tracer("t").Start(ctx, "write order")
	enqueue(tctx, t, pool, s, true, &pubsub.Message{Key: "c1", Data: []byte("created"), Attributes: map[string]string{"a": "1"}})
	span.End()

	rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
	defer rcancel()
	var got *pubsub.Message
	_ = sub.Receive(rctx, func(_ context.Context, d pubsub.Delivery) {
		got = d.Message()
		d.Ack()
		rcancel()
	})
	if got == nil {
		t.Fatal("relay did not publish (LISTEN/NOTIFY wake-up)")
	}
	if string(got.Data) != "created" || got.Key != "c1" || got.Attributes["a"] != "1" || got.Attributes[outbox.IDAttribute] != "1" {
		t.Fatalf("got %+v", got)
	}
	if tpHdr := got.Attributes["traceparent"]; len(tpHdr) < 36 || tpHdr[3:35] != span.SpanContext().TraceID().String() {
		t.Fatalf("trace context not carried through the outbox: %q", tpHdr)
	}
	cancel()
	wg.Wait()
}

func TestConcurrentRelaysKeepKeyOrder(t *testing.T) {
	s, pool := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	broker := memory.New()
	sub, _ := broker.Subscribe("orders", "test")

	const keys, perKey = 5, 20
	for i := range perKey {
		var msgs []*pubsub.Message
		for k := range keys {
			msgs = append(msgs, &pubsub.Message{Key: fmt.Sprint("k", k), Data: []byte(fmt.Sprint(i))})
		}
		enqueue(ctx, t, pool, s, true, msgs...)
	}
	var wg sync.WaitGroup
	for range 3 {
		r, err := outbox.NewRelay(s, broker, outbox.WithBatchSize(3), outbox.WithPollInterval(time.Millisecond), outbox.WithLogger(quiet))
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() { _ = r.Run(ctx) })
	}
	got := map[string][]int{}
	n := 0
	rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
	defer rcancel()
	_ = sub.Receive(rctx, func(_ context.Context, d pubsub.Delivery) {
		m := d.Message()
		var v int
		_, _ = fmt.Sscan(string(m.Data), &v)
		got[m.Key] = append(got[m.Key], v)
		d.Ack()
		if n++; n == keys*perKey {
			rcancel()
		}
	})
	cancel()
	wg.Wait()
	if n != keys*perKey {
		t.Fatalf("received %d of %d", n, keys*perKey)
	}
	for k, seq := range got {
		if !slices.IsSorted(seq) || len(seq) != perKey {
			t.Fatalf("key %s order %v", k, seq)
		}
	}
}
