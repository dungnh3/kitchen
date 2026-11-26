// Package pgxgorm opens a *gorm.DB on top of an existing *pgxpool.Pool.
//
// gorm and pgx then share one pool: one set of connections, one pool size to
// tune, and the pool's otelpgx tracer (set by pgx.Open) traces gorm queries
// too, so no gorm tracing plugin is needed. Logging goes through
// github.com/dungnh3/kitchen/log/gormlog.
//
//	pool, _ := pgx.Open(ctx, pgx.Config{URL: dsn})
//	db, _ := pgxgorm.Open(pool, pgxgorm.Options{})
package pgxgorm

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log/gormlog"
	"github.com/dungnh3/kitchen/pgx"
)

// Options configures Open.
type Options struct {
	// Log configures the gorm logger. nil means the gormlog defaults (errors
	// and queries slower than 200ms to log.Default(), SQL without parameter
	// values) with IgnoreRecordNotFound set, so First() misses are not logged
	// as errors.
	Log *gormlog.Config
	// Config is the base gorm config. Its Logger, when set, wins over Log.
	Config *gorm.Config
}

// Open returns a *gorm.DB using pool. Closing the returned DB's *sql.DB does
// not close pool; the caller keeps owning pool.
func Open(pool *pgxpool.Pool, opts Options) (*gorm.DB, error) {
	if pool == nil {
		return nil, errors.New("pgxgorm: nil pool")
	}
	cfg := &gorm.Config{}
	if opts.Config != nil {
		c := *opts.Config
		cfg = &c
	}
	if cfg.Logger == nil {
		lc := gormlog.Config{IgnoreRecordNotFound: true}
		if opts.Log != nil {
			lc = *opts.Log
		}
		cfg.Logger = gormlog.New(lc)
	}
	sqlDB := stdlib.OpenDBFromPool(pool)
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), cfg)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("pgxgorm: open: %w", err)
	}
	return db, nil
}

// MapError is pgx.MapError plus gorm.ErrRecordNotFound → errs.ErrNotFound.
func MapError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errs.Wrap(err, errs.ErrNotFound)
	}
	return pgx.MapError(err)
}
