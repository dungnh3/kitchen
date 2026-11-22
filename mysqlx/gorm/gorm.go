// Package mysqlgorm opens a *gorm.DB on top of an existing *sql.DB from
// github.com/dungnh3/kitchen/mysqlx.
//
// gorm then shares that database/sql pool and its otelsql tracing, so no gorm
// tracing plugin is needed. Logging goes through
// github.com/dungnh3/kitchen/log/gormlog.
//
//	db, _ := mysqlx.Open(ctx, mysqlx.Config{DSN: dsn})
//	gdb, _ := mysqlgorm.Open(db, mysqlgorm.Options{})
package mysqlgorm

import (
	"database/sql"
	"errors"
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log/gormlog"
	"github.com/dungnh3/kitchen/mysqlx"
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

// Open returns a *gorm.DB using db. gorm's DB() returns this same *sql.DB:
// the caller keeps owning it and closes it once.
func Open(db *sql.DB, opts Options) (*gorm.DB, error) {
	if db == nil {
		return nil, errors.New("mysqlgorm: nil db")
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
	gdb, err := gorm.Open(mysql.New(mysql.Config{Conn: db}), cfg)
	if err != nil {
		return nil, fmt.Errorf("mysqlgorm: open: %w", err)
	}
	return gdb, nil
}

// MapError is mysqlx.MapError plus gorm.ErrRecordNotFound → errs.ErrNotFound.
func MapError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errs.Wrap(err, errs.ErrNotFound)
	}
	return mysqlx.MapError(err)
}
