package pggorm_test

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
	"github.com/dungnh3/kitchen/pg"
	pggorm "github.com/dungnh3/kitchen/pg/gorm"
)

func ExampleOpen() {
	ctx := context.Background()
	pool, err := pg.Open(ctx, pg.Config{URL: os.Getenv("DATABASE_URL")})
	if err != nil {
		return
	}
	defer pool.Close() // pggorm never closes the pool

	db, err := pggorm.Open(pool, pggorm.Options{Log: &gormlog.Config{
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

// pggorm.MapError adds gorm.ErrRecordNotFound to pg.MapError.
func ExampleMapError() {
	err := pggorm.MapError(fmt.Errorf("first order: %w", gorm.ErrRecordNotFound))
	fmt.Println(errors.Is(err, errs.ErrNotFound))
	// Output: true
}
