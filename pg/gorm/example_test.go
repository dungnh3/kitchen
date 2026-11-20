package pggorm_test

import (
	"context"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	pggorm "github.com/dungnh3/kitchen/pg/gorm"
)

func ExampleOpen() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL")) // or pg.Open from the core module
	if err != nil {
		return
	}
	defer pool.Close()

	db, err := pggorm.Open(pool, pggorm.Options{Logger: slog.Default()})
	if err != nil {
		return
	}
	var count int64
	db.WithContext(ctx).Table("orders").Count(&count)
}
