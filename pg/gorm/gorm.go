// Package pggorm opens a *gorm.DB on top of an existing *pgxpool.Pool.
//
// gorm and pgx then share one pool: one set of connections, one pool size to
// tune, and the pool's otelpgx tracer traces gorm queries too, so no gorm
// tracing plugin is needed.
//
//	pool, _ := pg.Open(ctx, pg.Config{URL: dsn})
//	db, _ := pggorm.Open(pool, pggorm.Options{Logger: logger})
package pggorm

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Options configures Open.
type Options struct {
	// Logger receives gorm logs (default slog.Default()).
	Logger *slog.Logger
	// LogLevel is gorm's level (default logger.Warn: errors and slow queries).
	LogLevel logger.LogLevel
	// SlowThreshold marks queries as slow (default 200ms).
	SlowThreshold time.Duration
	// LogParameters includes query parameters in logs (default false: may
	// contain personal data).
	LogParameters bool
	// Config is the base gorm config; its Logger is replaced unless set.
	Config *gorm.Config
}

// Open returns a *gorm.DB using pool. Closing the returned DB's *sql.DB does
// not close pool; the caller keeps owning pool.
func Open(pool *pgxpool.Pool, opts Options) (*gorm.DB, error) {
	if pool == nil {
		return nil, errors.New("pggorm: nil pool")
	}
	cfg := &gorm.Config{}
	if opts.Config != nil {
		c := *opts.Config
		cfg = &c
	}
	if cfg.Logger == nil {
		l := opts.Logger
		if l == nil {
			l = slog.Default()
		}
		level := opts.LogLevel
		if level == 0 {
			level = logger.Warn
		}
		cfg.Logger = logger.NewSlogLogger(l, logger.Config{
			SlowThreshold:             durOr(opts.SlowThreshold, 200*time.Millisecond),
			LogLevel:                  level,
			IgnoreRecordNotFoundError: true,
			ParameterizedQueries:      !opts.LogParameters,
		})
	}
	sqlDB := stdlib.OpenDBFromPool(pool)
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), cfg)
	if err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("pggorm: open: %w", err)
	}
	return db, nil
}

func durOr(d, def time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return def
}
