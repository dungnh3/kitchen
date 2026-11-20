// Package migrate applies versioned SQL migrations to PostgreSQL.
//
// It wraps golang-migrate v4 (pgx/v5 driver) and keeps its formats:
// files named "<version>_<name>.up.sql" / ".down.sql" and the
// schema_migrations(version bigint, dirty boolean) table, so databases
// migrated by the golang-migrate CLI can switch without any state change.
//
// Differences from calling golang-migrate directly:
//   - [Migrator.Down] reverts exactly n migrations (golang-migrate's Down
//     reverts all of them).
//   - [Migrator.Close] never closes the caller's pool.
//   - ErrNoChange is not an error; a dirty database is reported as
//     [*DirtyError] and never fixed automatically.
//   - Every method takes a context; cancelling it stops after the migration
//     in progress.
//
// Concurrent runs (several pods starting at once) are serialised with a
// Postgres advisory lock.
package migrate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// ErrNilVersion is returned by Version when no migration has been applied.
var ErrNilVersion = migrate.ErrNilVersion

// DirtyError reports that a previous migration to Version failed half-way.
// Fix the schema by hand, then call Force with the last good version.
type DirtyError struct{ Version int }

func (e *DirtyError) Error() string {
	return fmt.Sprintf("migrate: database is dirty at version %d; fix it manually, then force the last good version", e.Version)
}

// Config configures a Migrator.
type Config struct {
	// Source holds the migration files. Required.
	Source fs.FS
	// Dir is the directory inside Source (default ".").
	Dir string
	// Table is the history table (default "schema_migrations"). It may be
	// schema-qualified ("app.schema_migrations").
	Table string
	// LockTimeout bounds waiting for the advisory lock (default 1m).
	LockTimeout time.Duration
	// StatementTimeout bounds each migration statement (0 = none).
	StatementTimeout time.Duration
	// Logger receives progress messages (default slog.Default()).
	Logger *slog.Logger
}

// Migrator applies migrations. It is not safe for concurrent use; create one
// per run and Close it. After a run was stopped by context cancellation the
// Migrator must not be reused.
type Migrator struct {
	m       *migrate.Migrate
	ownPool *pgxpool.Pool
	log     *slog.Logger
}

// New prepares a Migrator that uses pool. Closing the Migrator does not close
// pool. The Migrator holds one pool connection until Close.
func New(pool *pgxpool.Pool, cfg Config) (*Migrator, error) {
	if pool == nil {
		return nil, errors.New("migrate: nil pool")
	}
	if cfg.Source == nil {
		return nil, errors.New("migrate: Config.Source is required")
	}
	if cfg.Dir == "" {
		cfg.Dir = "."
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.LockTimeout <= 0 {
		cfg.LockTimeout = time.Minute
	}
	src, err := iofs.New(cfg.Source, cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("migrate: source: %w", err)
	}
	// A separate *sql.DB over the pool: the driver closes it on Close, which
	// does not close the pool.
	sqlDB := stdlib.OpenDBFromPool(pool)
	drv, err := pgxmigrate.WithInstance(sqlDB, &pgxmigrate.Config{
		MigrationsTable:       cfg.Table,
		MigrationsTableQuoted: false,
		StatementTimeout:      cfg.StatementTimeout,
	})
	if err != nil {
		_ = src.Close()
		_ = sqlDB.Close()
		return nil, fmt.Errorf("migrate: database driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", drv)
	if err != nil {
		_ = src.Close()
		_ = drv.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	m.LockTimeout = cfg.LockTimeout
	m.Log = slogLogger{cfg.Logger}
	return &Migrator{m: m, log: cfg.Logger}, nil
}

// Open is New with a pool it creates from databaseURL and owns; Close closes
// that pool. Intended for CLIs and jobs.
func Open(ctx context.Context, databaseURL string, cfg Config) (*Migrator, error) {
	pc, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("migrate: parse database URL: %w", err)
	}
	pc.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("migrate: connect: %w", err)
	}
	mg, err := New(pool, cfg)
	if err != nil {
		pool.Close()
		return nil, err
	}
	mg.ownPool = pool
	return mg, nil
}

// Close releases the source, the pinned connection and, for Open, the pool.
func (mg *Migrator) Close() error {
	srcErr, dbErr := mg.m.Close()
	if mg.ownPool != nil {
		mg.ownPool.Close()
	}
	return errors.Join(srcErr, dbErr)
}

// run executes op, stopping gracefully when ctx is cancelled.
func (mg *Migrator) run(ctx context.Context, name string, op func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
			mg.m.GracefulStop <- true
		case <-done:
		}
	}()
	err := op()
	close(done)
	<-stopped
	select { // drop a stop request that arrived after op finished
	case <-mg.m.GracefulStop:
	default:
	}
	var de migrate.ErrDirty
	switch {
	case err == nil, errors.Is(err, migrate.ErrNoChange):
		if cerr := ctx.Err(); cerr != nil {
			return fmt.Errorf("migrate: %s stopped: %w", name, cerr)
		}
		return nil
	case errors.As(err, &de):
		return &DirtyError{Version: de.Version}
	default:
		return fmt.Errorf("migrate: %s: %w", name, err)
	}
}

// Up applies all pending migrations.
func (mg *Migrator) Up(ctx context.Context) error {
	return mg.run(ctx, "up", mg.m.Up)
}

// Down reverts exactly n applied migrations (n >= 1).
func (mg *Migrator) Down(ctx context.Context, n int) error {
	if n < 1 {
		return fmt.Errorf("migrate: down: n must be >= 1, got %d", n)
	}
	return mg.run(ctx, "down", func() error { return mg.m.Steps(-n) })
}

// To migrates up or down to version.
func (mg *Migrator) To(ctx context.Context, version uint) error {
	return mg.run(ctx, "to", func() error { return mg.m.Migrate(version) })
}

// Version returns the current version and whether it is dirty. It returns
// ErrNilVersion when nothing has been applied.
func (mg *Migrator) Version(ctx context.Context) (version uint, dirty bool, err error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	return mg.m.Version()
}

// Force sets the recorded version without running migrations and clears the
// dirty flag. version -1 means "no migration applied". Use it only after
// repairing a dirty database by hand.
func (mg *Migrator) Force(ctx context.Context, version int) error {
	return mg.run(ctx, "force", func() error { return mg.m.Force(version) })
}

type slogLogger struct{ l *slog.Logger }

func (s slogLogger) Printf(format string, v ...any) {
	s.l.Info("migrate: " + strings.TrimSpace(fmt.Sprintf(format, v...)))
}

func (s slogLogger) Verbose() bool { return false }
