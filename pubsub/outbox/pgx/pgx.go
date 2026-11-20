// Package pgx implements an [outbox.Store] on PostgreSQL with
// github.com/jackc/pgx/v5.
//
// Write messages in the same transaction as the business data:
//
//	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
//	    if _, err := tx.Exec(ctx, "INSERT INTO orders ...", ...); err != nil {
//	        return err
//	    }
//	    return store.Enqueue(ctx, tx, "orders", msg)
//	})
//
// and run an [outbox.Relay] (optionally with [Store.Listen] for low latency).
// Rows are claimed with FOR UPDATE SKIP LOCKED; for a non-empty ordering key
// only the oldest row is claimable, so per-key order holds with any number of
// relays. Sent rows are deleted.
package pgx

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/outbox"
)

// Option configures a Store.
type Option func(*Store)

// WithTable sets the table name, optionally schema-qualified (default
// "kitchen_outbox"). It must match [A-Za-z_][A-Za-z0-9_]* per part.
func WithTable(name string) Option { return func(s *Store) { s.table = name } }

// WithPropagator sets the propagator used to store the trace context of the
// enqueuing transaction in the message attributes (default: the global one).
func WithPropagator(p propagation.TextMapPropagator) Option { return func(s *Store) { s.prop = p } }

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(s *Store) { s.log = l } }

// Store is a Postgres outbox store.
type Store struct {
	pool    *pgxpool.Pool
	table   string
	ident   string // sanitized table identifier
	channel string // sanitized NOTIFY channel identifier
	chanRaw string
	prop    propagation.TextMapPropagator
	log     *slog.Logger
	wake    chan struct{}
}

var (
	_ outbox.Store = (*Store)(nil)
	_ outbox.Waker = (*Store)(nil)
)

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}(\.[A-Za-z_][A-Za-z0-9_]{0,62})?$`)

// New returns a Store using pool, which the caller owns.
func New(pool *pgxpool.Pool, opts ...Option) (*Store, error) {
	if pool == nil {
		return nil, errors.New("outbox/pgx: nil pool")
	}
	s := &Store{pool: pool, table: "kitchen_outbox", wake: make(chan struct{}, 1)}
	for _, o := range opts {
		o(s)
	}
	if !identRE.MatchString(s.table) {
		return nil, fmt.Errorf("outbox/pgx: invalid table name %q", s.table)
	}
	s.ident = pgx.Identifier(strings.Split(s.table, ".")).Sanitize()
	s.chanRaw = strings.ReplaceAll(s.table, ".", "_") + "_events"
	s.channel = pgx.Identifier{s.chanRaw}.Sanitize()
	if s.prop == nil {
		s.prop = otel.GetTextMapPropagator()
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	return s, nil
}

// Schema returns the DDL for the outbox table and its indexes.
func (s *Store) Schema() string {
	t := s.ident
	base := strings.ReplaceAll(s.table, ".", "_")
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic         text        NOT NULL,
    ordering_key  text        NOT NULL DEFAULT '',
    payload       bytea       NOT NULL,
    attributes    jsonb       NOT NULL DEFAULT '{}',
    attempts      integer     NOT NULL DEFAULT 0,
    available_at  timestamptz NOT NULL DEFAULT now(),
    claimed_until timestamptz,
    last_error    text,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS %[2]s ON %[1]s (available_at, id);
CREATE INDEX IF NOT EXISTS %[3]s ON %[1]s (ordering_key, id) WHERE ordering_key <> '';`,
		t, pgx.Identifier{base + "_due_idx"}.Sanitize(), pgx.Identifier{base + "_key_idx"}.Sanitize())
}

// CreateTable executes Schema. Prefer running Schema through your migration
// tool in production.
func (s *Store) CreateTable(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, s.Schema())
	return err
}

// Enqueue inserts msgs for topic within tx and notifies listening relays on
// commit. The trace context of ctx is stored in the attributes (unless a
// message already carries one). msgs are not modified.
func (s *Store) Enqueue(ctx context.Context, tx pgx.Tx, topic string, msgs ...*pubsub.Message) error {
	if tx == nil || topic == "" {
		return errors.New("outbox/pgx: nil tx or empty topic")
	}
	if len(msgs) == 0 {
		return nil
	}
	keys := make([]string, len(msgs))
	payloads := make([][]byte, len(msgs))
	attrs := make([]string, len(msgs))
	for i, m := range msgs {
		if m == nil {
			return fmt.Errorf("outbox/pgx: message %d is nil", i)
		}
		a := maps.Clone(m.Attributes)
		if a == nil {
			a = map[string]string{}
		}
		if !slices.ContainsFunc(s.prop.Fields(), func(f string) bool { _, ok := a[f]; return ok }) {
			s.prop.Inject(ctx, propagation.MapCarrier(a))
		}
		b, err := json.Marshal(a)
		if err != nil {
			return err
		}
		keys[i], payloads[i], attrs[i] = m.Key, m.Data, string(b)
		if payloads[i] == nil {
			payloads[i] = []byte{}
		}
	}
	q := fmt.Sprintf(`INSERT INTO %s (topic, ordering_key, payload, attributes)
SELECT $1, u.k, u.p, u.a::jsonb FROM unnest($2::text[], $3::bytea[], $4::text[]) WITH ORDINALITY AS u(k, p, a, n)
ORDER BY u.n`, s.ident)
	if _, err := tx.Exec(ctx, q, topic, keys, payloads, attrs); err != nil {
		return fmt.Errorf("outbox/pgx: enqueue: %w", err)
	}
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, '')", s.chanRaw); err != nil {
		return fmt.Errorf("outbox/pgx: notify: %w", err)
	}
	return nil
}

// Claim implements [outbox.Store].
func (s *Store) Claim(ctx context.Context, limit int, lease time.Duration) ([]outbox.Record, error) {
	q := fmt.Sprintf(`WITH cand AS (
    SELECT o.id FROM %[1]s o
    WHERE o.available_at <= now()
      AND (o.claimed_until IS NULL OR o.claimed_until < now())
      AND (o.ordering_key = '' OR o.id = (SELECT min(h.id) FROM %[1]s h WHERE h.ordering_key = o.ordering_key))
    ORDER BY o.id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE %[1]s t SET claimed_until = now() + ($2::bigint * interval '1 millisecond'), attempts = t.attempts + 1
FROM cand WHERE t.id = cand.id
RETURNING t.id, t.topic, t.ordering_key, t.payload, t.attributes, t.attempts`, s.ident)
	rows, err := s.pool.Query(ctx, q, limit, lease.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("outbox/pgx: claim: %w", err)
	}
	recs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outbox.Record, error) {
		var r outbox.Record
		var key string
		var payload []byte
		var attrs map[string]string
		if err := row.Scan(&r.ID, &r.Topic, &key, &payload, &attrs, &r.Attempts); err != nil {
			return r, err
		}
		r.Message = &pubsub.Message{Key: key, Data: payload, Attributes: attrs}
		return r, nil
	})
	if err != nil {
		return nil, fmt.Errorf("outbox/pgx: claim: %w", err)
	}
	slices.SortFunc(recs, func(a, b outbox.Record) int { return cmp.Compare(a.ID, b.ID) })
	return recs, nil
}

// Sent implements [outbox.Store]: sent rows are deleted.
func (s *Store) Sent(ctx context.Context, ids []int64) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = ANY($1)", s.ident), ids)
	if err != nil {
		return fmt.Errorf("outbox/pgx: sent: %w", err)
	}
	return nil
}

// Failed implements [outbox.Store].
func (s *Store) Failed(ctx context.Context, id int64, cause error, retryAt time.Time) error {
	msg := ""
	if cause != nil {
		msg = cause.Error()
		if len(msg) > 1000 {
			msg = msg[:1000]
		}
	}
	_, err := s.pool.Exec(ctx, fmt.Sprintf(
		"UPDATE %s SET available_at = $2, claimed_until = NULL, last_error = $3 WHERE id = $1", s.ident), id, retryAt, msg)
	if err != nil {
		return fmt.Errorf("outbox/pgx: failed: %w", err)
	}
	return nil
}

// Wake implements [outbox.Waker]. It fires only while Listen runs.
func (s *Store) Wake() <-chan struct{} { return s.wake }

// Listen LISTENs for enqueue notifications on a dedicated connection and
// signals Wake until ctx is cancelled (then it returns nil). It reconnects
// with backoff on errors.
func (s *Store) Listen(ctx context.Context) error {
	backoff := 100 * time.Millisecond
	for {
		err := s.listenOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		s.log.Warn("outbox/pgx: listen failed", slog.Any("error", err), slog.Duration("retry_in", backoff))
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func (s *Store) listenOnce(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// A connection that was LISTENing must not go back to the pool.
	defer func() { _ = conn.Hijack().Close(context.Background()) }()
	if _, err := conn.Exec(ctx, "LISTEN "+s.channel); err != nil {
		return err
	}
	s.signal() // catch up on anything enqueued before LISTEN
	for {
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			return err
		}
		s.signal()
	}
}

func (s *Store) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
