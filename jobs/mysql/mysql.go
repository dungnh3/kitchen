// Package mysql is the MySQL 8 driver of
// [github.com/dungnh3/kitchen/jobs], on a *sql.DB from
// [github.com/dungnh3/kitchen/mysqlx.Open].
//
//	drv, err := jobsmysql.New(db, jobsmysql.WithTable("emails_jobs"))
//	client, err := jobs.NewClient[*sql.Tx](drv, jobs.WithRegistry(reg))
//
//	tx, err := db.BeginTx(ctx, nil)
//	// business writes ...
//	_, err = client.InsertTx(ctx, tx, SendWelcome{UserID: id}, nil)
//	err = tx.Commit()
//
// Jobs are claimed with FOR UPDATE SKIP LOCKED in a READ COMMITTED
// transaction, so MySQL 8.0.1 or later is required. MySQL has no
// notifications: idle workers poll ([jobs.WithPollInterval], default 1s), but
// a client wakes its own fetchers when it inserts. The database must be
// opened with parseTime (mysqlx does) and all times are UTC. Create the table
// with the SQL of [Driver.Schema] in a migration.
package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/dungnh3/kitchen/jobs"
	"github.com/dungnh3/kitchen/mysqlx"
)

// Option configures a Driver.
type Option func(*Driver)

// WithTable sets the table name, optionally qualified by a database
// (default "jobs"). Each part must match [A-Za-z_][A-Za-z0-9_]{0,39}.
// Several drivers on several tables give several independent queues in one
// database.
func WithTable(name string) Option { return func(d *Driver) { d.table = name } }

// Driver is a MySQL job store. It implements [jobs.Driver] for *sql.Tx.
type Driver struct {
	db    *sql.DB
	table string
	ident string
}

var _ jobs.Driver[*sql.Tx] = (*Driver)(nil)

var identRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,39}(\.[A-Za-z_][A-Za-z0-9_]{0,39})?$`)

// New returns a Driver using db, which the caller owns.
func New(db *sql.DB, opts ...Option) (*Driver, error) {
	if db == nil {
		return nil, errors.New("jobs/mysql: nil db")
	}
	d := &Driver{db: db, table: "jobs"}
	for _, o := range opts {
		o(d)
	}
	if !identRE.MatchString(d.table) {
		return nil, fmt.Errorf("jobs/mysql: invalid table name %q", d.table)
	}
	parts := strings.Split(d.table, ".")
	for i, p := range parts {
		parts[i] = "`" + p + "`" // the name matched identRE: no backtick can be in it
	}
	d.ident = strings.Join(parts, ".")
	return d, nil
}

// Schema returns the DDL of the job table and its indexes. It is idempotent.
// Run it from a migration.
func (d *Driver) Schema() string {
	base := strings.ReplaceAll(d.table, ".", "_")
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %[1]s (\n"+
		"    id           BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,\n"+
		"    kind         VARCHAR(255) NOT NULL,\n"+
		"    queue        VARCHAR(64)  NOT NULL DEFAULT 'default',\n"+
		"    args         JSON         NOT NULL,\n"+
		"    metadata     JSON         NOT NULL,\n"+
		"    state        VARCHAR(16)  NOT NULL DEFAULT 'available',\n"+
		"    priority     SMALLINT     NOT NULL DEFAULT 1,\n"+
		"    attempt      INT          NOT NULL DEFAULT 0,\n"+
		"    max_attempts INT          NOT NULL,\n"+
		"    scheduled_at DATETIME(6)  NOT NULL,\n"+
		"    locked_until DATETIME(6)  NULL,\n"+
		"    claim        BIGINT       NOT NULL DEFAULT 0,\n"+
		"    errors       JSON         NOT NULL,\n"+
		"    unique_key   VARCHAR(255) NULL,\n"+
		"    unique_keep  BOOLEAN      NOT NULL DEFAULT FALSE,\n"+
		"    created_at   DATETIME(6)  NOT NULL,\n"+
		"    finalized_at DATETIME(6)  NULL,\n"+
		"    UNIQUE KEY `%[2]s_unique_idx` (unique_key),\n"+
		"    KEY `%[2]s_fetch_idx` (queue, state, priority, scheduled_at, id),\n"+
		"    KEY `%[2]s_finalized_idx` (state, finalized_at)\n"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;", d.ident, base)
}

// CreateTable executes Schema. Prefer a migration in production.
func (d *Driver) CreateTable(ctx context.Context) error {
	_, err := d.db.ExecContext(ctx, d.Schema())
	return mysqlx.MapError(err)
}

func (d *Driver) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return mysqlx.MapError(err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return mysqlx.MapError(err)
	}
	return mysqlx.MapError(tx.Commit())
}

// Insert implements [jobs.Store].
func (d *Driver) Insert(ctx context.Context, in []jobs.NewJob) (out []jobs.InsertResult, err error) {
	err = d.inTx(ctx, func(tx *sql.Tx) (err error) {
		out, err = d.InsertTx(ctx, tx, in)
		return err
	})
	return out, err
}

// InsertTx implements [jobs.Driver]. tx may use any isolation level.
func (d *Driver) InsertTx(ctx context.Context, tx *sql.Tx, in []jobs.NewJob) ([]jobs.InsertResult, error) {
	if tx == nil {
		return nil, errors.New("jobs/mysql: nil tx")
	}
	q := fmt.Sprintf( //nolint:gosec // d.ident matched identRE.
		"INSERT INTO %s (kind, queue, args, metadata, priority, max_attempts, scheduled_at, unique_key, unique_keep, errors, created_at)\n"+
			"VALUES (?, ?, ?, ?, ?, ?, DATE_ADD(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND), ?, ?, '[]', UTC_TIMESTAMP(6))\n"+
			"ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)", d.ident)
	out := make([]jobs.InsertResult, len(in))
	for i, n := range in {
		var key any
		if n.UniqueKey != "" {
			key = n.UniqueKey
		}
		res, err := tx.ExecContext(ctx, q, n.Kind, n.Queue, string(n.Args), string(n.Metadata), n.Priority, n.MaxAttempts,
			n.Delay.Microseconds(), key, n.KeepUnique)
		if err != nil {
			return nil, mysqlx.MapError(err)
		}
		if out[i].ID, err = res.LastInsertId(); err != nil {
			return nil, mysqlx.MapError(err)
		}
		// An insert changes one row; an update that sets id to its own
		// value changes none.
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, mysqlx.MapError(err)
		}
		out[i].Duplicate = affected != 1
	}
	return out, nil
}

const cols = `id, kind, queue, args, metadata, state, priority, attempt, max_attempts, scheduled_at, created_at,
    finalized_at, claim, unique_key, errors`

type scanner interface{ Scan(dest ...any) error }

// scan reads the columns of [cols], then extra.
func scan(r scanner, extra ...any) (jobs.Row, error) {
	var (
		row       jobs.Row
		args, md  []byte
		errs      []byte
		state     string
		finalized sql.NullTime
		key       sql.NullString
	)
	dest := append([]any{
		&row.ID, &row.Kind, &row.Queue, &args, &md, &state, &row.Priority, &row.Attempt, &row.MaxAttempts,
		&row.ScheduledAt, &row.CreatedAt, &finalized, &row.Claim, &key, &errs,
	}, extra...)
	if err := r.Scan(dest...); err != nil {
		return jobs.Row{}, err
	}
	row.RawArgs, row.Metadata, row.State = args, md, jobs.State(state)
	row.FinalizedAt = finalized.Time
	row.UniqueKey = key.String
	if len(errs) > 0 {
		if err := json.Unmarshal(errs, &row.Errors); err != nil {
			return jobs.Row{}, fmt.Errorf("decode errors column: %w", err)
		}
	}
	return row, nil
}

// query runs q in tx and reads every row with read.
func query[T any](ctx context.Context, tx *sql.Tx, read func(scanner) (T, error), q string, args ...any) ([]T, error) {
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []T
	for rows.Next() {
		v, err := read(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func readID(r scanner) (id int64, err error) { return id, r.Scan(&id) }

func readRef(r scanner) (ref jobs.Ref, err error) { return ref, r.Scan(&ref.ID, &ref.Claim) }

func placeholders(n int) string { return strings.TrimSuffix(strings.Repeat("?,", n), ",") }

func ints(ids []int64) []any {
	out := make([]any, len(ids))
	for i, id := range ids {
		out[i] = id
	}
	return out
}

// Claim implements [jobs.Store].
func (d *Driver) Claim(ctx context.Context, queue string, kinds []string, limit int, lease time.Duration) ([]jobs.Claimed, error) {
	if limit < 1 || len(kinds) == 0 {
		return nil, nil
	}
	var out []jobs.Claimed
	err := d.inTx(ctx, func(tx *sql.Tx) error {
		out = nil
		args := []any{queue}
		for _, k := range kinds {
			args = append(args, k)
		}
		args = append(args, limit)
		ids, err := query(ctx, tx, readID, fmt.Sprintf("SELECT id FROM %s\n"+
			"WHERE queue = ? AND kind IN (%s)\n"+
			"  AND ((state = 'available' AND scheduled_at <= UTC_TIMESTAMP(6))\n"+
			"    OR (state = 'running' AND locked_until < UTC_TIMESTAMP(6) AND attempt < max_attempts))\n"+
			"ORDER BY priority, scheduled_at, id LIMIT ? FOR UPDATE SKIP LOCKED", d.ident, placeholders(len(kinds))), args...)
		if err != nil || len(ids) == 0 {
			return err
		}
		in := placeholders(len(ids))
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET state = 'running', attempt = attempt + 1, claim = claim + 1,\n"+
			"    locked_until = DATE_ADD(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND)\n"+
			"WHERE id IN (%s)", d.ident, in), append([]any{lease.Microseconds()}, ints(ids)...)...); err != nil {
			return err
		}
		out, err = query(ctx, tx, func(r scanner) (jobs.Claimed, error) {
			var lag int64
			row, err := scan(r, &lag)
			return jobs.Claimed{Row: row, Lag: max(time.Duration(lag)*time.Microsecond, 0)}, err
		}, fmt.Sprintf("SELECT %s, TIMESTAMPDIFF(MICROSECOND, scheduled_at, UTC_TIMESTAMP(6)) FROM %s\n"+
			"WHERE id IN (%s) ORDER BY priority, scheduled_at, id", cols, d.ident, in), ints(ids)...)
		return err
	})
	return out, err
}

// Extend implements [jobs.Store].
func (d *Driver) Extend(ctx context.Context, refs []jobs.Ref, lease time.Duration) ([]jobs.Ref, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	pairs := strings.TrimSuffix(strings.Repeat("(?, ?),", len(refs)), ",")
	args := make([]any, 0, 2*len(refs))
	for _, r := range refs {
		args = append(args, r.ID, r.Claim)
	}
	var lost []jobs.Ref
	err := d.inTx(ctx, func(tx *sql.Tx) error {
		lost = nil
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET locked_until = DATE_ADD(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND)\n"+
			"WHERE (id, claim) IN (%s) AND state = 'running'", d.ident, pairs), append([]any{lease.Microseconds()}, args...)...); err != nil {
			return err
		}
		heldRefs, err := query(ctx, tx, readRef, fmt.Sprintf("SELECT id, claim FROM %s WHERE (id, claim) IN (%s) AND state = 'running'", d.ident, pairs), args...)
		if err != nil {
			return err
		}
		held := map[jobs.Ref]bool{}
		for _, r := range heldRefs {
			held[r] = true
		}
		for _, r := range refs {
			if !held[r] {
				lost = append(lost, r)
			}
		}
		return nil
	})
	return lost, err
}

// Settle implements [jobs.Store].
func (d *Driver) Settle(ctx context.Context, ref jobs.Ref, s jobs.Settlement) error {
	var errJSON any
	if s.Error != nil {
		b, err := json.Marshal(s.Error)
		if err != nil {
			return err
		}
		errJSON = string(b)
	}
	giveBack := 0
	if s.GiveBackAttempt {
		giveBack = 1
	}
	state := string(s.State)
	// The assignments read state through parameters, never the column: MySQL
	// evaluates them left to right on the updated row.
	q := fmt.Sprintf( //nolint:gosec // d.ident matched identRE.
		"UPDATE %s SET\n"+
			"    state = ?,\n"+
			"    attempt = attempt - ?,\n"+
			"    scheduled_at = IF(? = 'available', DATE_ADD(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND), scheduled_at),\n"+
			"    locked_until = NULL,\n"+
			"    finalized_at = IF(? = 'available', NULL, UTC_TIMESTAMP(6)),\n"+
			"    unique_key = IF(? = 'available' OR unique_keep, unique_key, NULL),\n"+
			"    errors = IF(? IS NULL, errors,\n"+
			"        JSON_ARRAY_APPEND(IF(JSON_LENGTH(errors) >= 10, JSON_REMOVE(errors, '$[0]'), errors), '$', CAST(? AS JSON)))\n"+
			"WHERE id = ? AND claim = ? AND state = 'running'", d.ident)
	res, err := d.db.ExecContext(ctx, q, state, giveBack, state, s.Delay.Microseconds(), state, state, errJSON, errJSON, ref.ID, ref.Claim)
	if err != nil {
		return mysqlx.MapError(err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return mysqlx.MapError(err)
	} else if n == 0 {
		return jobs.ErrClaimLost
	}
	return nil
}

// Reap implements [jobs.Store].
func (d *Driver) Reap(ctx context.Context) (int, error) {
	res, err := d.db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET\n"+
		"    errors = JSON_ARRAY_APPEND(errors, '$', JSON_OBJECT(\n"+
		"        'at', DATE_FORMAT(UTC_TIMESTAMP(6), '%%Y-%%m-%%dT%%H:%%i:%%s.%%fZ'),\n"+
		"        'attempt', attempt, 'error', 'lease expired on the last attempt')),\n"+
		"    unique_key = IF(unique_keep, unique_key, NULL),\n"+
		"    finalized_at = UTC_TIMESTAMP(6), locked_until = NULL, state = 'discarded'\n"+
		"WHERE state = 'running' AND locked_until < UTC_TIMESTAMP(6) AND attempt >= max_attempts\n"+
		"ORDER BY id LIMIT 1000", d.ident))
	if err != nil {
		return 0, mysqlx.MapError(err)
	}
	n, err := res.RowsAffected()
	return int(n), mysqlx.MapError(err)
}

// Cleanup implements [jobs.Store].
func (d *Driver) Cleanup(ctx context.Context, p jobs.CleanupParams) (int, error) {
	var n int64
	err := d.inTx(ctx, func(tx *sql.Tx) error {
		ids, err := query(ctx, tx, readID, fmt.Sprintf("SELECT id FROM %s\n"+
			"WHERE (state = 'completed' AND finalized_at <= DATE_SUB(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND))\n"+
			"   OR (state IN ('cancelled', 'discarded') AND finalized_at <= DATE_SUB(UTC_TIMESTAMP(6), INTERVAL ? MICROSECOND))\n"+
			"ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED", d.ident), p.Completed.Microseconds(), p.Discarded.Microseconds(), p.Limit)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		res, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", d.ident, placeholders(len(ids))), ints(ids)...)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	return int(n), err
}

// Get implements [jobs.Store].
func (d *Driver) Get(ctx context.Context, id int64) (*jobs.Row, error) {
	row, err := scan(d.db.QueryRowContext(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE id = ?", cols, d.ident), id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, jobs.ErrNotFound
	}
	if err != nil {
		return nil, mysqlx.MapError(err)
	}
	return &row, nil
}
