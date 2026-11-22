package mysqlgorm_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"gorm.io/gorm"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/log/gormlog"
	"github.com/dungnh3/kitchen/mysqlx"
	mysqlgorm "github.com/dungnh3/kitchen/mysqlx/gorm"
)

type User struct {
	ID    int64
	Email string `gorm:"size:255;uniqueIndex"`
}

func ExampleOpen() {
	ctx := context.Background()
	db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: os.Getenv("MYSQL_DSN")})
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }() // close the *sql.DB once; gorm does not own it

	gdb, err := mysqlgorm.Open(db, mysqlgorm.Options{})
	if err != nil {
		return
	}
	var u User
	err = mysqlgorm.MapError(gdb.WithContext(ctx).First(&u, "email = ?", "a@b.c").Error)
	if errors.Is(err, errs.ErrNotFound) {
		return // the handler returns err: NOT_FOUND on gRPC, 404 on REST
	}

	// sqlc (or plain database/sql) on the same pool and tracing:
	//   q := store.New(db)
	var n int
	_ = db.QueryRowContext(ctx, "SELECT count(*) FROM users").Scan(&n)
}

func ExampleOpen_logging() {
	ctx := context.Background()
	logger := log.Default() // the process logger, set up by log.Setup in main

	db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: os.Getenv("MYSQL_DSN")}, mysqlx.WithLogger(logger))
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()

	gdb, err := mysqlgorm.Open(db, mysqlgorm.Options{
		Log: &gormlog.Config{
			Logger:               logger,
			SlowThreshold:        100 * time.Millisecond,
			IgnoreRecordNotFound: true, // a Log replaces the defaults: keep this set
		},
		// Leave TranslateError off: it hides the MySQL error MapError reads.
		Config: &gorm.Config{SkipDefaultTransaction: true},
	})
	if err != nil {
		return
	}
	_ = gdb
}

func ExampleMapError() {
	err := mysqlgorm.MapError(fmt.Errorf("find user: %w", gorm.ErrRecordNotFound))
	fmt.Println(errors.Is(err, errs.ErrNotFound), errs.HTTPStatus(err))
	// Output: true 404
}
