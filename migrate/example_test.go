package migrate_test

import (
	"context"
	"embed"
	"errors"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dungnh3/kitchen/migrate"
)

//go:embed testdata/ok/*.sql
var migrations embed.FS

func ExampleNew() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL")) // or pg.Open from the core module
	if err != nil {
		return
	}
	defer pool.Close()

	m, err := migrate.New(pool, migrate.Config{Source: migrations, Dir: "testdata/ok", Logger: slog.Default()})
	if err != nil {
		return
	}
	defer func() { _ = m.Close() }() // does not close pool

	var dirty *migrate.DirtyError
	if err := m.Up(ctx); errors.As(err, &dirty) {
		slog.Error("database needs manual repair", "version", dirty.Version)
	}
}
