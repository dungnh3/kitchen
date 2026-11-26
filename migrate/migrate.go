// Package migrate applies versioned migrations to PostgreSQL or MySQL.
//
// It is a thin layer over goose v3's Provider API (github.com/pressly/goose):
//
//   - SQL files are "<version>_<name>.sql" with "-- +goose Up" and
//     "-- +goose Down" sections; Go migrations come from Config.GoMigrations.
//   - Each migration runs in its own transaction (unless the file says
//     "-- +goose NO TRANSACTION"), so a failed migration rolls back and is
//     not recorded; goose has no "dirty" state. MySQL commits DDL
//     implicitly, so a failed MySQL migration can leave some of its DDL
//     applied, and so can a NO TRANSACTION migration.
//   - Concurrent runs (several pods starting at once) are serialised: a
//     Postgres advisory lock, or a lock table on MySQL. A run waits up to
//     Config.LockTimeout for the lock, then fails.
//   - Every method except Close takes a context; cancelling it stops the
//     run and rolls back the running migration's transaction, if any.
//   - Closing a Migrator does not close a pool or *sql.DB passed to
//     NewPostgres or NewMySQL.
//
// History is kept in goose_db_version (Config.Table). The schema_migrations
// table of golang-migrate is not read; docs/migration/data-utils.md
// describes a one-time conversion.
package migrate

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/mysqlx"
	"github.com/dungnh3/kitchen/pgx"
)

// Dialect selects the database.
type Dialect string

// Supported dialects.
const (
	Postgres Dialect = "postgres"
	MySQL    Dialect = "mysql"
)

// Config configures a Migrator.
type Config struct {
	// Source holds the SQL migration files. Required unless GoMigrations
	// is set.
	Source fs.FS
	// Dir is the directory inside Source (default ".").
	Dir string
	// GoMigrations are migrations written in Go ([NewGoMigration],
	// [NewGoMigrationNoTx]).
	GoMigrations []*GoMigration
	// Table is the history table (default "goose_db_version").
	Table string
	// LockTimeout bounds waiting for the migration lock (default 1m).
	LockTimeout time.Duration
	// AllowOutOfOrder applies missing older migrations instead of failing
	// (useful when branches merge migrations with interleaved versions).
	AllowOutOfOrder bool
	// Logger receives progress messages (nil: log.Default()).
	Logger *log.Logger
}

// GoMigration is a migration written in Go. Build it with [NewGoMigration]
// or [NewGoMigrationNoTx]; callers never need to import goose.
type GoMigration = goose.Migration

// MigrationStatus is one row of [Migrator.Status].
type MigrationStatus = goose.MigrationStatus

// States reported in MigrationStatus.State.
const (
	StatePending = goose.StatePending
	StateApplied = goose.StateApplied
)

// NewGoMigration returns a Go migration whose up and down run inside the
// migration's transaction. A nil up or down does nothing but record the
// version change.
func NewGoMigration(version int64, up, down func(context.Context, *sql.Tx) error) *GoMigration {
	var d *goose.GoFunc
	if down != nil {
		d = &goose.GoFunc{RunTx: down}
	}
	return goose.NewGoMigration(version, &goose.GoFunc{RunTx: up}, d)
}

// NewGoMigrationNoTx returns a Go migration that runs outside a transaction,
// for statements that cannot run in one (CREATE INDEX CONCURRENTLY). A
// failure leaves the database partially migrated. A nil up or down does
// nothing but record the version change.
func NewGoMigrationNoTx(version int64, up, down func(context.Context, *sql.DB) error) *GoMigration {
	var d *goose.GoFunc
	if down != nil {
		d = &goose.GoFunc{RunDB: down}
	}
	return goose.NewGoMigration(version, &goose.GoFunc{RunDB: up}, d)
}

// Migrator applies migrations. Create one per run and Close it.
type Migrator struct {
	p      *goose.Provider
	logger *log.Logger
	close  func() error // releases what the Migrator owns
}

// NewPostgres prepares a Migrator on pool, locked with a Postgres advisory
// lock. Closing the Migrator does not close pool.
func NewPostgres(pool *pgxpool.Pool, cfg Config) (*Migrator, error) {
	if pool == nil {
		return nil, errors.New("migrate: nil pool")
	}
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, lockSeconds(cfg.LockTimeout)))
	if err != nil {
		return nil, fmt.Errorf("migrate: lock: %w", err)
	}
	// A separate *sql.DB over the pool: closing it does not close the pool.
	db := stdlib.OpenDBFromPool(pool)
	m, err := newMigrator(goose.DialectPostgres, db, cfg, goose.WithSessionLocker(locker))
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	m.close = db.Close
	return m, nil
}

// NewMySQL prepares a Migrator on db (e.g. from mysqlx.Open), locked with a
// lock table (goose_lock). Closing the Migrator does not close db.
func NewMySQL(db *sql.DB, cfg Config) (*Migrator, error) {
	if db == nil {
		return nil, errors.New("migrate: nil db")
	}
	locker, err := lock.NewMySQLTableLocker(
		lock.WithTableLockTimeout(time.Second, lockSeconds(cfg.LockTimeout)),
		lock.WithTableLogger(log.OrDefault(cfg.Logger)),
	)
	if err != nil {
		return nil, fmt.Errorf("migrate: lock: %w", err)
	}
	m, err := newMigrator(goose.DialectMySQL, db, cfg, goose.WithLocker(locker))
	if err != nil {
		return nil, err
	}
	m.close = func() error { return nil }
	return m, nil
}

// lockSeconds is the number of 1s lock probes for timeout (default 1m, at
// least 1).
func lockSeconds(timeout time.Duration) uint64 {
	if timeout <= 0 {
		timeout = time.Minute
	}
	return uint64(max(timeout/time.Second, 1)) //nolint:gosec // positive by construction.
}

func newMigrator(d goose.Dialect, db *sql.DB, cfg Config, lockOpt goose.ProviderOption) (*Migrator, error) {
	logger := log.OrDefault(cfg.Logger)
	var fsys fs.FS
	if cfg.Source != nil {
		var err error
		if fsys, err = fs.Sub(cfg.Source, cmp.Or(cfg.Dir, ".")); err != nil {
			return nil, fmt.Errorf("migrate: source: %w", err)
		}
	} else if len(cfg.GoMigrations) == 0 {
		return nil, errors.New("migrate: Config.Source or GoMigrations is required")
	}
	opts := []goose.ProviderOption{
		lockOpt,
		goose.WithDisableGlobalRegistry(true), // no package-level state
		goose.WithAllowOutofOrder(cfg.AllowOutOfOrder),
		goose.WithSlog(logger),
	}
	if cfg.Table != "" {
		opts = append(opts, goose.WithTableName(cfg.Table))
	}
	if len(cfg.GoMigrations) > 0 {
		opts = append(opts, goose.WithGoMigrations(cfg.GoMigrations...))
	}
	p, err := goose.NewProvider(d, db, fsys, opts...)
	if err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Migrator{p: p, logger: logger}, nil
}

// Open connects with pgx.Open or mysqlx.Open (same TLS defaults and
// validation as services use) and returns a Migrator that owns the
// connection; Close closes it. Intended for CLIs and jobs.
func Open(ctx context.Context, d Dialect, dsn string, cfg Config) (*Migrator, error) {
	switch d {
	case Postgres, "":
		pool, err := pgx.Open(ctx, pgx.Config{URL: dsn, MaxConns: 2, ApplicationName: "kmigrate"},
			pgx.WithLogger(cfg.Logger), pgx.WithoutPoolStats())
		if err != nil {
			return nil, fmt.Errorf("migrate: %w", err)
		}
		m, err := NewPostgres(pool, cfg)
		if err != nil {
			pool.Close()
			return nil, err
		}
		dbClose := m.close
		m.close = func() error { err := dbClose(); pool.Close(); return err }
		return m, nil
	case MySQL:
		db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: dsn, MaxOpenConns: 2},
			mysqlx.WithLogger(cfg.Logger), mysqlx.WithoutPoolStats())
		if err != nil {
			return nil, fmt.Errorf("migrate: %w", err)
		}
		m, err := NewMySQL(db, cfg)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		m.close = db.Close
		return m, nil
	default:
		return nil, fmt.Errorf("migrate: unknown dialect %q", d)
	}
}

// Close releases what the Migrator owns: for Open, the connection. It never
// closes a pool or *sql.DB passed to NewPostgres or NewMySQL.
func (m *Migrator) Close() error { return m.close() }

// Up applies all pending migrations.
func (m *Migrator) Up(ctx context.Context) error {
	res, err := m.p.Up(ctx)
	m.logResults(ctx, res)
	return wrap("up", err)
}

// Down reverts exactly n applied migrations (n >= 1), newest first. It stops
// early, without error, when nothing is left to revert.
func (m *Migrator) Down(ctx context.Context, n int) error {
	if n < 1 {
		return fmt.Errorf("migrate: down: n must be >= 1, got %d", n)
	}
	for range n {
		res, err := m.p.Down(ctx)
		if errors.Is(err, goose.ErrNoNextVersion) {
			return nil
		}
		if res != nil {
			m.logResults(ctx, []*goose.MigrationResult{res})
		}
		if err != nil {
			return wrap("down", err)
		}
	}
	return nil
}

// To migrates up or down so that version is the newest applied migration
// (0 reverts everything).
func (m *Migrator) To(ctx context.Context, version int64) error {
	if version < 0 {
		return fmt.Errorf("migrate: to: version must be >= 0, got %d", version)
	}
	cur, err := m.p.GetDBVersion(ctx)
	if err != nil {
		return wrap("to", err)
	}
	var res []*goose.MigrationResult
	switch {
	case version > cur:
		res, err = m.p.UpTo(ctx, version)
	case version < cur:
		res, err = m.p.DownTo(ctx, version)
	}
	m.logResults(ctx, res)
	return wrap("to", err)
}

// Version returns the newest applied version (0 when none).
func (m *Migrator) Version(ctx context.Context) (int64, error) {
	v, err := m.p.GetDBVersion(ctx)
	return v, wrap("version", err)
}

// Status lists every known migration and whether it is applied.
func (m *Migrator) Status(ctx context.Context) ([]*MigrationStatus, error) {
	st, err := m.p.Status(ctx)
	return st, wrap("status", err)
}

// HasPending reports whether migrations are waiting to be applied (useful
// for a readiness or CI check).
func (m *Migrator) HasPending(ctx context.Context) (bool, error) {
	ok, err := m.p.HasPending(ctx)
	return ok, wrap("pending", err)
}

func (m *Migrator) logResults(ctx context.Context, res []*goose.MigrationResult) {
	for _, r := range res {
		if r == nil || r.Source == nil || r.Error != nil {
			continue
		}
		m.logger.InfoContext(ctx, "migrate: applied",
			"direction", r.Direction, "version", r.Source.Version, "path", r.Source.Path, "duration", r.Duration)
	}
}

func wrap(op string, err error) error {
	if err == nil || errors.Is(err, goose.ErrNoNextVersion) {
		return nil
	}
	return fmt.Errorf("migrate: %s: %w", op, err)
}
