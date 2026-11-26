package pgxgorm_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"gorm.io/gorm"

	"github.com/dungnh3/kitchen/errs"
	pgxgorm "github.com/dungnh3/kitchen/pgx/gorm"
)

func TestMapError(t *testing.T) {
	if !errors.Is(pgxgorm.MapError(gorm.ErrRecordNotFound), errs.ErrNotFound) {
		t.Fatal("record not found")
	}
	if !errors.Is(pgxgorm.MapError(&pgconn.PgError{Code: "23505"}), errs.ErrAlreadyExists) {
		t.Fatal("duplicate")
	}
}

func TestOpenNilPool(t *testing.T) {
	if _, err := pgxgorm.Open(nil, pgxgorm.Options{}); err == nil {
		t.Fatal("want error")
	}
}

type Item struct {
	ID   int64
	Name string
}

func TestSharedPool(t *testing.T) {
	if testing.Short() || os.Getenv("KITCHEN_SKIP_DOCKER") != "" {
		t.Skip("docker tests disabled")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("app"), tcpostgres.WithUsername("app"), tcpostgres.WithPassword("app"),
		tcpostgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatal(err)
	}
	url, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	db, err := pgxgorm.Open(pool, pgxgorm.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&Item{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&Item{Name: "a"}).Error; err != nil {
		t.Fatal(err)
	}
	// Closing gorm's *sql.DB must leave the pool usable.
	sqlDB, _ := db.DB()
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM items").Scan(&n); err != nil || n != 1 {
		t.Fatalf("count = %d, %v", n, err)
	}
}
