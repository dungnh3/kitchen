// Package pgx implements an [outbox.Store] on PostgreSQL with
// github.com/jackc/pgx/v5.
//
// Write messages in the same transaction as the business data:
//
//	err := jpgx.BeginFunc(ctx, pool, func(tx jpgx.Tx) error {
//	    if _, err := tx.Exec(ctx, "INSERT INTO orders ...", ...); err != nil {
//	        return err
//	    }
//	    return store.Enqueue(ctx, tx, "orders", msg)
//	})
//
// and run an [outbox.Relay] (optionally with [Store.Listen] for low latency).
// Rows are claimed with FOR UPDATE SKIP LOCKED; for a non-empty ordering key
// a relay claims a run of rows only by locking the key's oldest row, so
// per-key order holds with any number of relays. Each claim gets a new
// claim_token, and Sent, Failed, Release and Dead change a row only while
// its claim_token is the one the caller claimed, so a relay whose lease
// lapsed cannot settle a row another relay has claimed since. Enqueue
// serialises writers of the same key with an advisory lock (see
// [Store.KeyLockID]), so per-key order is commit order. Sent rows are
// deleted; dead rows (see [outbox.Store]) are kept with dead_at set.
package pgx

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	jpgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pgx"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/outbox"
	"github.com/dungnh3/kitchen/timex"
)

// Option configures a Store.
type Option func(*Store)

// WithTable sets the table name, optionally schema-qualified (default
// "kitchen_outbox"). It must match [A-Za-z_][A-Za-z0-9_]* per part.
func WithTable(name string) Option { return func(s *Store) { s.table = name } }

// WithPropagator sets the propagator used to store the trace context of the
// enqueuing transaction in the message attributes (default: the global one).
func WithPropagator(p propagation.TextMapPropagator) Option { return func(s *Store) { s.prop = p } }

// WithLogger sets the logger (nil means log.Default()).
func WithLogger(l *log.Logger) Option { return func(s *Store) { s.log = l } }

// Store is a Postgres outbox store.
type Store struct {
	pool    *pgxpool.Pool
	table   string
	ident   string // sanitized table identifier
	channel string // sanitized NOTIFY channel identifier
	chanRaw string
	prop    propagation.TextMapPropagator
	log     *log.Logger
	wake    chan struct{}

	mu     sync.Mutex
	cursor string // ordering key after which the next claim's key scan starts

	listenOnceFn func(ctx context.Context, onListen func()) error // listenOnce; replaced in tests
}

var (
	_ outbox.Store = (*Store)(nil)
	_ outbox.Waker = (*Store)(nil)
)

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}(\.[A-Za-z_][A-Za-z0-9_]{0,62})?$`)

// New returns a Store using pool, which the caller owns. Create it with
// [github.com/dungnh3/kitchen/pgx.Open] (tracing, metrics, sane pool limits).
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
	s.ident = jpgx.Identifier(strings.Split(s.table, ".")).Sanitize()
	s.chanRaw = strings.ReplaceAll(s.table, ".", "_") + "_events"
	s.channel = jpgx.Identifier{s.chanRaw}.Sanitize()
	if s.prop == nil {
		s.prop = otel.GetTextMapPropagator()
	}
	s.log = log.OrDefault(s.log)
	s.listenOnceFn = s.listenOnce
	return s, nil
}

// Schema returns the DDL for the outbox table and its indexes. It is
// idempotent: on an existing table it adds the claim_token column and any
// missing index.
//
// The store reads and writes claim_token, so the column must exist before
// the store runs against the table. Schema builds indexes with a lock that
// blocks writes; on a large existing table, add the column and the unkeyed
// index without blocking writes:
//
//	ALTER TABLE <table> ADD COLUMN IF NOT EXISTS claim_token bigint NOT NULL DEFAULT 0;
//	CREATE INDEX CONCURRENTLY IF NOT EXISTS <table>_free_idx ON <table> (id) WHERE ordering_key = '' AND dead_at IS NULL;
//	DROP INDEX CONCURRENTLY IF EXISTS <table>_due_idx;
//
// Adding a column with a constant default does not rewrite the table
// (Postgres 11+). claim_token fences settle calls to the claim (see
// [outbox.Ref]); only claims that incremented it are fenced.
// <table>_free_idx serves unkeyed rows. The store does not use
// <table>_due_idx; the DROP statement removes it where it exists.
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
    dead_at       timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now(),
    claim_token   bigint      NOT NULL DEFAULT 0
);
ALTER TABLE %[1]s ADD COLUMN IF NOT EXISTS claim_token bigint NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS %[2]s ON %[1]s (ordering_key, id) WHERE ordering_key <> '' AND dead_at IS NULL;
CREATE INDEX IF NOT EXISTS %[3]s ON %[1]s (id) WHERE ordering_key = '' AND dead_at IS NULL;`,
		t, jpgx.Identifier{base + "_key_idx"}.Sanitize(), jpgx.Identifier{base + "_free_idx"}.Sanitize())
}

// CreateTable executes Schema. Prefer running Schema through your migration
// tool in production.
func (s *Store) CreateTable(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, s.Schema())
	return pgx.MapError(err)
}

// Enqueue inserts msgs for topic within tx and notifies listening relays on
// commit. The trace context of ctx is stored in the attributes (unless a
// message already carries one). msgs are not modified.
//
// Before inserting, Enqueue takes a transaction-level advisory lock for each
// distinct non-empty ordering key of msgs (see [KeyLockID]), in ascending
// lock-ID order. Writers of the same key are thereby serialised until
// commit, so for every key the row IDs follow the commit order and a relay
// never sees a later record of a key before an earlier one commits. The
// cost: a transaction that enqueues for a key waits for any other open
// transaction that enqueued for that key. To avoid deadlocks, enqueue all
// keyed messages of a transaction in one call, or across calls take keys in
// a consistent order (Postgres aborts one side of a deadlock with SQLSTATE
// 40P01).
func (s *Store) Enqueue(ctx context.Context, tx jpgx.Tx, topic string, msgs ...*pubsub.Message) error {
	if tx == nil || topic == "" {
		return errors.New("outbox/pgx: nil tx or empty topic")
	}
	if len(msgs) == 0 {
		return nil
	}
	keys := make([]string, len(msgs))
	payloads := make([][]byte, len(msgs))
	attrs := make([]string, len(msgs))
	var locks []int64
	for i, m := range msgs {
		if m == nil {
			return errs.Wrap(fmt.Errorf("outbox/pgx: message %d is nil", i), errs.ErrInvalidArgument)
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
		if m.Key != "" {
			locks = append(locks, s.KeyLockID(m.Key))
		}
		if payloads[i] == nil {
			payloads[i] = []byte{}
		}
	}
	if len(locks) > 0 {
		slices.Sort(locks)
		locks = slices.Compact(locks)
		// unnest emits the array in order, so the locks are taken in
		// ascending order.
		if _, err := tx.Exec(ctx, "SELECT count(pg_advisory_xact_lock(l)) FROM unnest($1::bigint[]) AS u(l)", locks); err != nil {
			return pgx.MapError(fmt.Errorf("outbox/pgx: lock keys: %w", err))
		}
	}
	q := fmt.Sprintf(`INSERT INTO %s (topic, ordering_key, payload, attributes)
SELECT $1, u.k, u.p, u.a::jsonb FROM unnest($2::text[], $3::bytea[], $4::text[]) WITH ORDINALITY AS u(k, p, a, n)
ORDER BY u.n`, s.ident)
	if _, err := tx.Exec(ctx, q, topic, keys, payloads, attrs); err != nil {
		return pgx.MapError(fmt.Errorf("outbox/pgx: enqueue: %w", err))
	}
	if _, err := tx.Exec(ctx, "SELECT pg_notify($1, '')", s.chanRaw); err != nil {
		return pgx.MapError(fmt.Errorf("outbox/pgx: notify: %w", err))
	}
	return nil
}

// keyLockNamespace prefixes the input of every key lock ID, so the IDs of
// different users of the 64-bit advisory lock space cannot be equal unless
// their inputs share this prefix.
const keyLockNamespace = "kitchen/pubsub/outbox\x00"

// KeyLockID returns the 64-bit Postgres advisory lock ID that Enqueue takes
// (with pg_advisory_xact_lock) for ordering key: the FNV-1a 64-bit hash of
// "kitchen/pubsub/outbox\x00" + table + "\x00" + key, as a signed bigint.
// The namespace prefix keeps these IDs apart from other advisory lock users
// of the database; the table name keeps outbox tables apart. A collision
// only serialises two unrelated keys, it never breaks order. Take the same
// lock to serialise other writers with Enqueue.
func (s *Store) KeyLockID(key string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(keyLockNamespace))
	_, _ = h.Write([]byte(s.table))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(key))
	return int64(h.Sum64()) //nolint:gosec // reinterpreting the hash bits as a bigint is intended
}

// claimQuery returns the claim statement: $1 limit, $2 lease in ms, $3 the
// key cursor, $4 the age window. It returns the claimed rows plus whether
// each was found by the key scan.
//
// Heads (the oldest live row of a key, due and unleased) come from two
// sources, so the cost does not grow with the rows queued behind a key:
//
//   - by_age: the oldest $4 due keyed rows in id order, kept if no older live
//     row of the key exists (an index probe of _key_idx). This keeps the
//     claim oldest-first.
//   - by_key: a loose index scan of _key_idx over the distinct keys after
//     the cursor $3, one probe per key, until $1 due heads are found. Keys
//     behind a long or blocked key (whose queued rows fill the age window)
//     are still served; the cursor rotates the scan over the key space.
//
// heads locks the chosen heads (FOR UPDATE SKIP LOCKED: a key whose head
// another relay is claiming is skipped). runs adds, for each locked head, the
// next rows of its key in id order while they are due, unleased and of the
// head's topic (bool_and stops the run at the first row that is not). Rows
// after the head are only claimed by the relay holding the head's lock, so
// runs can neither overlap nor overtake each other. free takes due unkeyed
// rows through _free_idx. The union is cut to $1 rows in id order, which
// keeps every run a prefix of itself.
func (s *Store) claimQuery() string {
	return fmt.Sprintf(`WITH RECURSIVE keys(k) AS (
    (SELECT n.ordering_key FROM %[1]s n
     WHERE n.ordering_key > $3 AND n.ordering_key <> '' AND n.dead_at IS NULL
     ORDER BY n.ordering_key LIMIT 1)
    UNION ALL
    SELECT (SELECT n.ordering_key FROM %[1]s n
            WHERE n.ordering_key > keys.k AND n.ordering_key <> '' AND n.dead_at IS NULL
            ORDER BY n.ordering_key LIMIT 1)
    FROM keys WHERE keys.k IS NOT NULL
), by_key AS (
    SELECT h.id FROM keys CROSS JOIN LATERAL (
        SELECT n.id, n.available_at, n.claimed_until FROM %[1]s n
        WHERE n.ordering_key = keys.k AND n.ordering_key <> '' AND n.dead_at IS NULL
        ORDER BY n.id LIMIT 1
    ) h
    WHERE keys.k IS NOT NULL AND h.available_at <= now()
      AND (h.claimed_until IS NULL OR h.claimed_until < now())
    LIMIT $1
), by_age AS (
    SELECT w.id FROM (
        SELECT o.id, o.ordering_key FROM %[1]s o
        WHERE o.ordering_key <> '' AND o.dead_at IS NULL
          AND o.available_at <= now()
          AND (o.claimed_until IS NULL OR o.claimed_until < now())
        ORDER BY o.id
        LIMIT $4
    ) w
    WHERE NOT EXISTS (SELECT 1 FROM %[1]s h
        WHERE h.ordering_key = w.ordering_key AND h.ordering_key <> ''
          AND h.dead_at IS NULL AND h.id < w.id)
), heads AS (
    SELECT o.id, o.ordering_key, o.topic, c.by_key FROM %[1]s o
    JOIN (SELECT u.id, bool_or(u.by_key) AS by_key FROM (
              SELECT id, false AS by_key FROM by_age UNION ALL SELECT id, true FROM by_key
          ) u GROUP BY u.id) c ON c.id = o.id
    WHERE o.dead_at IS NULL AND o.available_at <= now()
      AND (o.claimed_until IS NULL OR o.claimed_until < now())
    ORDER BY o.id
    LIMIT $1
    FOR UPDATE OF o SKIP LOCKED
), runs AS (
    SELECT r.id FROM heads hd CROSS JOIN LATERAL (
        SELECT f.id, bool_and(f.ok) OVER (ORDER BY f.id) AS contiguous FROM (
            SELECT n.id, (n.topic = hd.topic AND n.available_at <= now()
                    AND (n.claimed_until IS NULL OR n.claimed_until < now())) AS ok
            FROM %[1]s n
            WHERE n.ordering_key = hd.ordering_key AND n.ordering_key <> ''
              AND n.dead_at IS NULL AND n.id > hd.id
            ORDER BY n.id
            LIMIT $1 - 1
        ) f
    ) r
    WHERE r.contiguous
), free AS (
    SELECT o.id FROM %[1]s o
    WHERE o.ordering_key = '' AND o.dead_at IS NULL
      AND o.available_at <= now()
      AND (o.claimed_until IS NULL OR o.claimed_until < now())
    ORDER BY o.id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
), cand AS (
    SELECT id, by_key FROM heads
    UNION ALL SELECT id, false FROM runs
    UNION ALL SELECT id, false FROM free
    ORDER BY id
    LIMIT $1
)
UPDATE %[1]s t SET claimed_until = now() + ($2::bigint * interval '1 millisecond'),
    attempts = t.attempts + 1, claim_token = t.claim_token + 1
FROM cand WHERE t.id = cand.id
RETURNING t.id, t.topic, t.ordering_key, t.payload, t.attributes, t.attempts, t.claim_token, cand.by_key`, s.ident)
}

// ageWindowFactor sizes the by_age window of a claim: limit times this.
const ageWindowFactor = 4

// Claim implements [outbox.Store].
func (s *Store) Claim(ctx context.Context, limit int, lease time.Duration) ([]outbox.Record, error) {
	s.mu.Lock()
	cursor := s.cursor
	s.mu.Unlock()
	rows, err := s.pool.Query(ctx, s.claimQuery(), limit, lease.Milliseconds(), cursor, limit*ageWindowFactor)
	if err != nil {
		return nil, pgx.MapError(fmt.Errorf("outbox/pgx: claim: %w", err))
	}
	next := ""
	recs, err := jpgx.CollectRows(rows, func(row jpgx.CollectableRow) (outbox.Record, error) {
		var r outbox.Record
		var key string
		var payload []byte
		var attrs map[string]string
		var byKey bool
		if err := row.Scan(&r.ID, &r.Topic, &key, &payload, &attrs, &r.Attempts, &r.Claim, &byKey); err != nil {
			return r, err
		}
		if byKey && key > next {
			next = key
		}
		r.Message = &pubsub.Message{Key: key, Data: payload, Attributes: attrs}
		return r, nil
	})
	if err != nil {
		return nil, pgx.MapError(fmt.Errorf("outbox/pgx: claim: %w", err))
	}
	// Continue the key scan after the last key it supplied; when it supplied
	// none, start over from the first key.
	s.mu.Lock()
	s.cursor = next
	s.mu.Unlock()
	slices.SortFunc(recs, func(a, b outbox.Record) int { return cmp.Compare(a.ID, b.ID) })
	return recs, nil
}

// Sent implements [outbox.Store]: sent rows are deleted.
func (s *Store) Sent(ctx context.Context, refs []outbox.Ref) error {
	return s.settle(ctx, "sent", refs, fmt.Sprintf(
		"DELETE FROM %s t USING unnest($1::bigint[], $2::bigint[]) AS u(id, claim) WHERE t.id = u.id AND t.claim_token = u.claim RETURNING t.id",
		s.ident))
}

// Failed implements [outbox.Store]. retryAt is applied as a delay from the
// database clock.
func (s *Store) Failed(ctx context.Context, ref outbox.Ref, cause error, retryAt time.Time) error {
	return s.settle(ctx, "failed", []outbox.Ref{ref}, s.fencedUpdate(
		"available_at = now() + ($3::bigint * interval '1 microsecond'), claimed_until = NULL, last_error = $4"),
		delayUntil(retryAt), truncErr(cause))
}

// Release implements [outbox.Store]: the rows are due again at retryAt
// (applied as a delay from the database clock) and the current attempt is
// not counted.
func (s *Store) Release(ctx context.Context, refs []outbox.Ref, cause error, retryAt time.Time) error {
	var lastErr *string
	if cause != nil {
		m := truncErr(cause)
		lastErr = &m
	}
	return s.settle(ctx, "release", refs, s.fencedUpdate(
		"available_at = now() + ($3::bigint * interval '1 microsecond'), claimed_until = NULL, attempts = greatest(t.attempts - 1, 0), last_error = coalesce($4, t.last_error)"),
		delayUntil(retryAt), lastErr)
}

// Dead implements [outbox.Store]: the row is kept with dead_at set and is
// never claimed again. Inspect dead rows with "WHERE dead_at IS NOT NULL";
// replay one by setting dead_at = NULL, attempts = 0.
func (s *Store) Dead(ctx context.Context, ref outbox.Ref, cause error) error {
	return s.settle(ctx, "dead", []outbox.Ref{ref}, s.fencedUpdate(
		"dead_at = now(), claimed_until = NULL, last_error = $3"), truncErr(cause))
}

// fencedUpdate returns an UPDATE of the rows in $1 (ids) whose claim_token
// still equals $2 (claims), returning the ids it changed.
func (s *Store) fencedUpdate(set string) string {
	return fmt.Sprintf("UPDATE %s t SET %s FROM unnest($1::bigint[], $2::bigint[]) AS u(id, claim) WHERE t.id = u.id AND t.claim_token = u.claim RETURNING t.id",
		s.ident, set)
}

// settle runs a fenced statement (see fencedUpdate) for refs and reports
// the refs it did not change as an [*outbox.ClaimLostError].
func (s *Store) settle(ctx context.Context, op string, refs []outbox.Ref, q string, args ...any) error {
	if len(refs) == 0 {
		return nil
	}
	ids := make([]int64, len(refs))
	claims := make([]int64, len(refs))
	for i, r := range refs {
		ids[i], claims[i] = r.ID, r.Claim
	}
	rows, err := s.pool.Query(ctx, q, append([]any{ids, claims}, args...)...)
	if err != nil {
		return pgx.MapError(fmt.Errorf("outbox/pgx: %s: %w", op, err))
	}
	done, err := jpgx.CollectRows(rows, jpgx.RowTo[int64])
	if err != nil {
		return pgx.MapError(fmt.Errorf("outbox/pgx: %s: %w", op, err))
	}
	if len(done) == len(refs) {
		return nil
	}
	var lost []int64
	for _, id := range ids {
		if !slices.Contains(done, id) {
			lost = append(lost, id)
		}
	}
	return &outbox.ClaimLostError{IDs: lost}
}

// delayUntil converts retryAt into a delay from now in microseconds, which
// the store adds to the database clock: claims compare available_at with
// the database's now(), so a skewed application clock cannot shift retries.
func delayUntil(retryAt time.Time) int64 {
	return max(time.Until(retryAt), 0).Microseconds()
}

func truncErr(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) > 1000 {
		msg = msg[:1000] // may split a rune; Postgres text must be valid UTF-8
	}
	return strings.ToValidUTF8(msg, "")
}

// Wake implements [outbox.Waker]. It fires only while Listen runs.
func (s *Store) Wake() <-chan struct{} { return s.wake }

// Listen LISTENs for enqueue notifications on a dedicated connection and
// signals Wake until ctx is cancelled (then it returns nil). It reconnects
// with backoff on errors; the backoff starts over once LISTEN succeeds.
func (s *Store) Listen(ctx context.Context) error {
	for attempt := 1; ; attempt++ {
		err := s.listenOnceFn(ctx, func() { attempt = 0 })
		if ctx.Err() != nil {
			return nil
		}
		backoff := timex.Backoff(100*time.Millisecond, 30*time.Second, attempt)
		s.log.WarnContext(ctx, "outbox/pgx: listen failed", log.Err(err), log.Duration("retry_in", backoff))
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}

// listenOnce LISTENs on one connection until an error; it calls onListen
// once LISTEN has succeeded.
func (s *Store) listenOnce(ctx context.Context, onListen func()) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// A connection that was LISTENing must not go back to the pool.
	defer func() { _ = conn.Hijack().Close(context.WithoutCancel(ctx)) }()
	if _, err := conn.Exec(ctx, "LISTEN "+s.channel); err != nil {
		return err
	}
	onListen()
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
