// Package pgx is the PostgreSQL driver of [github.com/dungnh3/kitchen/jobs],
// on a pool from [github.com/dungnh3/kitchen/pgx.Open].
//
//	drv, err := jobspgx.New(pool, jobspgx.WithTable("emails_jobs"))
//	client, err := jobs.NewClient[jpgx.Tx](drv, jobs.WithRegistry(reg))
//
//	err = jpgx.BeginFunc(ctx, pool, func(tx jpgx.Tx) error {
//	    // business writes ...
//	    _, err := client.InsertTx(ctx, tx, SendWelcome{UserID: id}, nil)
//	    return err
//	})
//
// Jobs are claimed with FOR UPDATE SKIP LOCKED. Every insert sends a NOTIFY
// when its transaction commits, and [Driver.Listen] (which the client runs)
// turns it into an immediate wake-up; polling remains the fallback. Create
// the table with the SQL of [Driver.Schema] in a migration.
package pgx

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	jpgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dungnh3/kitchen/jobs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pgx"
	"github.com/dungnh3/kitchen/timex"
)

// Option configures a Driver.
type Option func(*Driver)

// WithTable sets the table name, optionally schema-qualified (default
// "jobs"). Each part must match [A-Za-z_][A-Za-z0-9_]{0,39}. Several
// drivers on several tables give several independent queues in one
// database.
func WithTable(name string) Option { return func(d *Driver) { d.table = name } }

// WithLogger sets the logger (nil means log.Default()).
func WithLogger(l *log.Logger) Option { return func(d *Driver) { d.log = l } }

// Driver is a PostgreSQL job store. It implements
// [jobs.Driver] for [jpgx.Tx], [jobs.Waker] and [jobs.Listener].
type Driver struct {
	pool    *pgxpool.Pool
	table   string
	ident   string
	channel string
	log     *log.Logger
	wake    chan struct{}

	listenOnceFn func(ctx context.Context, onListen func()) error // replaced in tests
}

var (
	_ jobs.Driver[jpgx.Tx] = (*Driver)(nil)
	_ jobs.Waker           = (*Driver)(nil)
	_ jobs.Listener        = (*Driver)(nil)
)

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,39}(\.[A-Za-z_][A-Za-z0-9_]{0,39})?$`)

// New returns a Driver using pool, which the caller owns.
func New(pool *pgxpool.Pool, opts ...Option) (*Driver, error) {
	if pool == nil {
		return nil, errors.New("jobs/pgx: nil pool")
	}
	d := &Driver{pool: pool, table: "jobs", wake: make(chan struct{}, 1)}
	for _, o := range opts {
		o(d)
	}
	if !identRE.MatchString(d.table) {
		return nil, fmt.Errorf("jobs/pgx: invalid table name %q", d.table)
	}
	d.ident = jpgx.Identifier(strings.Split(d.table, ".")).Sanitize()
	d.channel = strings.ReplaceAll(d.table, ".", "_") + "_jobs"
	d.log = log.OrDefault(d.log)
	d.listenOnceFn = d.listenOnce
	return d, nil
}

// Schema returns the DDL of the job table and its indexes. It is idempotent.
// Run it from a migration.
func (d *Driver) Schema() string {
	base := strings.ReplaceAll(d.table, ".", "_")
	idx := func(suffix string) string { return jpgx.Identifier{base + suffix}.Sanitize() }
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
    id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind         varchar(255) NOT NULL,
    queue        varchar(64) NOT NULL DEFAULT 'default',
    args         jsonb       NOT NULL DEFAULT '{}',
    metadata     jsonb       NOT NULL DEFAULT '{}',
    state        varchar(16) NOT NULL DEFAULT 'available',
    priority     smallint    NOT NULL DEFAULT 1,
    attempt      integer     NOT NULL DEFAULT 0,
    max_attempts integer     NOT NULL,
    scheduled_at timestamptz NOT NULL DEFAULT now(),
    locked_until timestamptz,
    claim        bigint      NOT NULL DEFAULT 0,
    errors       jsonb       NOT NULL DEFAULT '[]',
    unique_key   varchar(255),
    unique_keep  boolean     NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL DEFAULT now(),
    finalized_at timestamptz
);
CREATE UNIQUE INDEX IF NOT EXISTS %[2]s ON %[1]s (unique_key);
CREATE INDEX IF NOT EXISTS %[3]s ON %[1]s (queue, priority, scheduled_at, id) WHERE state IN ('available', 'running');
CREATE INDEX IF NOT EXISTS %[4]s ON %[1]s (finalized_at) WHERE finalized_at IS NOT NULL;`,
		d.ident, idx("_unique_idx"), idx("_fetch_idx"), idx("_finalized_idx"))
}

// CreateTable executes Schema.
func (d *Driver) CreateTable(ctx context.Context) error {
	_, err := d.pool.Exec(ctx, d.Schema())
	return pgx.MapError(err)
}

// Insert implements [jobs.Store].
func (d *Driver) Insert(ctx context.Context, in []jobs.NewJob) (out []jobs.InsertResult, err error) {
	err = jpgx.BeginFunc(ctx, d.pool, func(tx jpgx.Tx) error {
		out, err = d.InsertTx(ctx, tx, in)
		return err
	})
	return out, pgx.MapError(err)
}

const insertSQL = `WITH ins AS (
    INSERT INTO %[1]s (kind, queue, args, metadata, priority, max_attempts, scheduled_at, unique_key, unique_keep)
    VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6, now() + $7::bigint * interval '1 microsecond', $8, $9)
    ON CONFLICT (unique_key) DO NOTHING
    RETURNING id)
SELECT id, false FROM ins
UNION ALL
SELECT id, true FROM %[1]s WHERE unique_key = $8 AND NOT EXISTS (SELECT 1 FROM ins)`

// insertTries bounds the repeats of an insert whose duplicate was deleted
// between the conflict and the lookup.
const insertTries = 3

// InsertTx implements [jobs.Driver]. It also sends a NOTIFY, delivered when
// tx commits.
func (d *Driver) InsertTx(ctx context.Context, tx jpgx.Tx, in []jobs.NewJob) ([]jobs.InsertResult, error) {
	if tx == nil {
		return nil, errors.New("jobs/pgx: nil tx")
	}
	q := fmt.Sprintf(insertSQL, d.ident)
	out := make([]jobs.InsertResult, len(in))
	for i, n := range in {
		var key *string
		if n.UniqueKey != "" {
			key = &n.UniqueKey
		}
		found := false
		for try := 0; try < insertTries && !found; try++ {
			err := tx.QueryRow(ctx, q, n.Kind, n.Queue, string(n.Args), string(n.Metadata), n.Priority, n.MaxAttempts,
				n.Delay.Microseconds(), key, n.KeepUnique).Scan(&out[i].ID, &out[i].Duplicate)
			switch {
			case err == nil:
				found = true
			case errors.Is(err, jpgx.ErrNoRows):
			default:
				return nil, pgx.MapError(err)
			}
		}
		if !found {
			return nil, fmt.Errorf("jobs/pgx: job %d: unique key %q conflicts with a row this transaction cannot see", i, n.UniqueKey)
		}
	}
	if len(in) > 0 {
		if _, err := tx.Exec(ctx, "SELECT pg_notify($1, '')", d.channel); err != nil {
			return nil, pgx.MapError(err)
		}
	}
	return out, nil
}

const cols = `id, kind, queue, args, metadata, state, priority, attempt, max_attempts, scheduled_at, created_at,
    finalized_at, claim, unique_key, errors`

// scanner is a pgx Row or Rows.
type scanner interface{ Scan(dest ...any) error }

// scan reads the columns of [cols], then extra.
func scan(r scanner, extra ...any) (jobs.Row, error) {
	var (
		row       jobs.Row
		args, md  []byte
		errs      []byte
		state     string
		finalized *time.Time
		key       *string
	)
	dest := append([]any{
		&row.ID, &row.Kind, &row.Queue, &args, &md, &state, &row.Priority, &row.Attempt, &row.MaxAttempts,
		&row.ScheduledAt, &row.CreatedAt, &finalized, &row.Claim, &key, &errs,
	}, extra...)
	if err := r.Scan(dest...); err != nil {
		return jobs.Row{}, err
	}
	row.RawArgs, row.Metadata = args, md
	var err error
	if row.State, err = jobs.ParseState(state); err != nil {
		return jobs.Row{}, err
	}
	if finalized != nil {
		row.FinalizedAt = *finalized
	}
	if key != nil {
		row.UniqueKey = *key
	}
	if len(errs) > 0 {
		if err := json.Unmarshal(errs, &row.Errors); err != nil {
			return jobs.Row{}, fmt.Errorf("decode errors column: %w", err)
		}
	}
	return row, nil
}

// Claim implements [jobs.Store].
func (d *Driver) Claim(ctx context.Context, queue string, kinds []string, limit int, lease time.Duration) ([]jobs.Claimed, error) {
	if limit < 1 || len(kinds) == 0 {
		return nil, nil
	}
	q := fmt.Sprintf(`WITH picked AS (
    SELECT id FROM %[1]s
    WHERE queue = $1 AND kind = ANY($2)
      AND ((state = 'available' AND scheduled_at <= now())
        OR (state = 'running' AND locked_until < now() AND attempt < max_attempts))
    ORDER BY priority, scheduled_at, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED)
UPDATE %[1]s AS j
SET state = 'running', attempt = j.attempt + 1, claim = j.claim + 1,
    locked_until = now() + $4::bigint * interval '1 microsecond'
FROM picked
WHERE j.id = picked.id
RETURNING %[2]s, (extract(epoch FROM (now() - j.scheduled_at)) * 1000000)::bigint`, d.ident, qualify("j", cols))
	rows, err := d.pool.Query(ctx, q, queue, kinds, limit, lease.Microseconds())
	if err != nil {
		return nil, pgx.MapError(err)
	}
	defer rows.Close()
	var out []jobs.Claimed
	for rows.Next() {
		var (
			row jobs.Row
			lag int64
		)
		row, err = scan(rows, &lag)
		if err != nil {
			return nil, pgx.MapError(err)
		}
		out = append(out, jobs.Claimed{Row: row, Lag: max(time.Duration(lag)*time.Microsecond, 0)})
	}
	if err := rows.Err(); err != nil {
		return nil, pgx.MapError(err)
	}
	slices.SortFunc(out, func(a, b jobs.Claimed) int {
		return cmp.Or(cmp.Compare(a.Priority, b.Priority), a.ScheduledAt.Compare(b.ScheduledAt), cmp.Compare(a.ID, b.ID))
	})
	return out, nil
}

// qualify prefixes every column of the comma-separated list with alias.
func qualify(alias, list string) string {
	parts := strings.Split(list, ",")
	for i, p := range parts {
		parts[i] = alias + "." + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}

// Extend implements [jobs.Store].
func (d *Driver) Extend(ctx context.Context, refs []jobs.Ref, lease time.Duration) ([]jobs.Ref, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	ids, claims := make([]int64, len(refs)), make([]int64, len(refs))
	for i, r := range refs {
		ids[i], claims[i] = r.ID, r.Claim
	}
	q := fmt.Sprintf(`UPDATE %s SET locked_until = now() + $3::bigint * interval '1 microsecond'
WHERE (id, claim) IN (SELECT * FROM unnest($1::bigint[], $2::bigint[])) AND state = 'running'
RETURNING id, claim`, d.ident)
	rows, err := d.pool.Query(ctx, q, ids, claims, lease.Microseconds())
	if err != nil {
		return nil, pgx.MapError(err)
	}
	extended := map[jobs.Ref]bool{}
	for rows.Next() {
		var ref jobs.Ref
		if err := rows.Scan(&ref.ID, &ref.Claim); err != nil {
			rows.Close()
			return nil, pgx.MapError(err)
		}
		extended[ref] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, pgx.MapError(err)
	}
	var lost []jobs.Ref
	for _, r := range refs {
		if !extended[r] {
			lost = append(lost, r)
		}
	}
	return lost, nil
}

// Settle implements [jobs.Store].
func (d *Driver) Settle(ctx context.Context, ref jobs.Ref, s jobs.Settlement) error {
	if err := s.Validate(); err != nil {
		return err
	}
	var errJSON *string
	if s.Error != nil {
		b, err := json.Marshal(s.Error)
		if err != nil {
			return err
		}
		str := string(b)
		errJSON = &str
	}
	giveBack := 0
	if s.GiveBackAttempt {
		giveBack = 1
	}
	q := fmt.Sprintf(`UPDATE %s SET
    state = $3::text,
    attempt = attempt - $4::int,
    scheduled_at = CASE WHEN $3::text = 'available' THEN now() + $5::bigint * interval '1 microsecond' ELSE scheduled_at END,
    locked_until = NULL,
    finalized_at = CASE WHEN $3::text = 'available' THEN NULL ELSE now() END,
    unique_key = CASE WHEN $3::text = 'available' OR unique_keep THEN unique_key ELSE NULL END,
    errors = CASE
        WHEN $6::text IS NULL THEN errors
        WHEN jsonb_array_length(errors) >= 10 THEN (errors - 0) || jsonb_build_array($6::text::jsonb)
        ELSE errors || jsonb_build_array($6::text::jsonb) END
WHERE id = $1 AND claim = $2 AND state = 'running'`, d.ident)
	tag, err := d.pool.Exec(ctx, q, ref.ID, ref.Claim, string(s.State), giveBack, s.Delay.Microseconds(), errJSON)
	if err != nil {
		return pgx.MapError(err)
	}
	if tag.RowsAffected() == 0 {
		return jobs.ErrClaimLost
	}
	return nil
}

// Reap implements [jobs.Store].
func (d *Driver) Reap(ctx context.Context) (int, error) {
	q := fmt.Sprintf(`UPDATE %[1]s SET state = 'discarded', finalized_at = now(), locked_until = NULL,
    unique_key = CASE WHEN unique_keep THEN unique_key ELSE NULL END,
    errors = errors || jsonb_build_array(jsonb_build_object(
        'at', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'attempt', attempt, 'error', 'lease expired on the last attempt'))
WHERE id IN (SELECT id FROM %[1]s WHERE state = 'running' AND locked_until < now() AND attempt >= max_attempts
             LIMIT 1000 FOR UPDATE SKIP LOCKED)`, d.ident)
	tag, err := d.pool.Exec(ctx, q)
	if err != nil {
		return 0, pgx.MapError(err)
	}
	return int(tag.RowsAffected()), nil
}

// Cleanup implements [jobs.Store].
func (d *Driver) Cleanup(ctx context.Context, p jobs.CleanupParams) (int, error) {
	q := fmt.Sprintf(`DELETE FROM %[1]s WHERE id IN (
    SELECT id FROM %[1]s
    WHERE (state = 'completed' AND finalized_at <= now() - $1::bigint * interval '1 microsecond')
       OR (state IN ('cancelled', 'discarded') AND finalized_at <= now() - $2::bigint * interval '1 microsecond')
    ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED)`, d.ident)
	tag, err := d.pool.Exec(ctx, q, p.Completed.Microseconds(), p.Discarded.Microseconds(), p.Limit)
	if err != nil {
		return 0, pgx.MapError(err)
	}
	return int(tag.RowsAffected()), nil
}

// Get implements [jobs.Store].
func (d *Driver) Get(ctx context.Context, id int64) (*jobs.Row, error) {
	row, err := scan(d.pool.QueryRow(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE id = $1", cols, d.ident), id))
	if errors.Is(err, jpgx.ErrNoRows) {
		return nil, jobs.ErrNotFound
	}
	if err != nil {
		return nil, pgx.MapError(err)
	}
	return &row, nil
}

// Wake implements [jobs.Waker].
func (d *Driver) Wake() <-chan struct{} { return d.wake }

// Listen LISTENs for the NOTIFY of inserts until ctx is cancelled (it then
// returns nil), reconnecting with backoff. The client runs it.
func (d *Driver) Listen(ctx context.Context) error {
	for attempt := 1; ; attempt++ {
		err := d.listenOnceFn(ctx, func() { attempt = 0 })
		if ctx.Err() != nil {
			return nil
		}
		backoff := timex.Backoff(100*time.Millisecond, 30*time.Second, attempt)
		d.log.WarnContext(ctx, "jobs/pgx: listen failed", log.Err(err), log.Duration("retry_in", backoff))
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}

func (d *Driver) listenOnce(ctx context.Context, onListen func()) error {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// A connection that was LISTENing must not go back to the pool.
	defer func() { _ = conn.Hijack().Close(context.WithoutCancel(ctx)) }()
	if _, err := conn.Exec(ctx, "LISTEN "+jpgx.Identifier{d.channel}.Sanitize()); err != nil {
		return err
	}
	onListen()
	d.signal() // catch up on anything inserted before LISTEN
	for {
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			return err
		}
		d.signal()
	}
}

func (d *Driver) signal() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}
