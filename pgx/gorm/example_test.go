package pgxgorm_test

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
	"github.com/dungnh3/kitchen/pgx"
	pgxgorm "github.com/dungnh3/kitchen/pgx/gorm"
)

func ExampleOpen() {
	ctx := context.Background()
	pool, err := pgx.Open(ctx, pgx.Config{URL: os.Getenv("DATABASE_URL")})
	if err != nil {
		return
	}
	defer pool.Close() // pgxgorm never closes the pool

	db, err := pgxgorm.Open(pool, pgxgorm.Options{Log: &gormlog.Config{
		Logger:               log.Default(),
		SlowThreshold:        500 * time.Millisecond,
		IgnoreRecordNotFound: true,
	}})
	if err != nil {
		return
	}
	var count int64
	db.WithContext(ctx).Table("orders").Count(&count)
}

// pgxgorm.MapError adds gorm.ErrRecordNotFound to pgx.MapError.
func ExampleMapError() {
	err := pgxgorm.MapError(fmt.Errorf("first order: %w", gorm.ErrRecordNotFound))
	fmt.Println(errors.Is(err, errs.ErrNotFound))
	// Output: true
}
