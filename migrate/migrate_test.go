package migrate_test

import (
	"context"
	"embed"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/dungnh3/kitchen/migrate"
)

//go:embed testdata
var testdata embed.FS

func TestNewValidation(t *testing.T) {
	if _, err := migrate.New(nil, migrate.Config{Source: testdata}); err == nil {
		t.Fatal("nil pool: want error")
	}
}

func startPostgres(t *testing.T) string {
	t.Helper()
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
	return url
}

func version(ctx context.Context, t *testing.T, m *migrate.Migrator) (uint, bool) {
	t.Helper()
	v, dirty, err := m.Version(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return v, dirty
}

func TestLifecycle(t *testing.T) {
	url := startPostgres(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	t.Run("up down to keep pool open", func(t *testing.T) {
		m, err := migrate.New(pool, migrate.Config{Source: testdata, Dir: "testdata/ok"})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := m.Version(ctx); !errors.Is(err, migrate.ErrNilVersion) {
			t.Fatalf("initial Version err = %v", err)
		}
		if err := m.Up(ctx); err != nil {
			t.Fatal(err)
		}
		if err := m.Up(ctx); err != nil { // no change is not an error
			t.Fatal(err)
		}
		if v, _ := version(ctx, t, m); v != 20250313100000 {
			t.Fatalf("after Up v = %d", v)
		}
		// Down reverts exactly one step (old library reverted everything).
		if err := m.Down(ctx, 1); err != nil {
			t.Fatal(err)
		}
		if v, _ := version(ctx, t, m); v != 20250304100000 {
			t.Fatalf("after Down(1) v = %d", v)
		}
		if err := m.Down(ctx, 0); err == nil {
			t.Fatal("Down(0): want error")
		}
		if err := m.To(ctx, 20250313100000); err != nil {
			t.Fatal(err)
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		// The caller's pool is still usable after Close.
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM items").Scan(&n); err != nil || n != 1 {
			t.Fatalf("pool after Close: n=%d err=%v", n, err)
		}
		var table string
		if err := pool.QueryRow(ctx, "SELECT to_regclass('schema_migrations')::text").Scan(&table); err != nil || table != "schema_migrations" {
			t.Fatalf("history table = %q, %v", table, err)
		}
	})

	t.Run("dirty is reported, force recovers", func(t *testing.T) {
		m, err := migrate.New(pool, migrate.Config{Source: testdata, Dir: "testdata/broken", Table: "broken_migrations"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = m.Close() }()
		if err := m.Up(ctx); err == nil {
			t.Fatal("broken Up: want error")
		}
		var de *migrate.DirtyError
		if err := m.Up(ctx); !errors.As(err, &de) || de.Version != 2 {
			t.Fatalf("second Up err = %v, want DirtyError{2}", err)
		}
		if v, dirty := version(ctx, t, m); v != 2 || !dirty {
			t.Fatalf("Version = %d dirty=%v", v, dirty)
		}
		if err := m.Force(ctx, 1); err != nil {
			t.Fatal(err)
		}
		if v, dirty := version(ctx, t, m); v != 1 || dirty {
			t.Fatalf("after Force = %d dirty=%v", v, dirty)
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		m, err := migrate.New(pool, migrate.Config{Source: testdata, Dir: "testdata/ok", Table: "cancel_migrations"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = m.Close() }()
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if err := m.Up(cctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("Open owns its pool", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "CREATE SCHEMA other"); err != nil {
			t.Fatal(err)
		}
		m, err := migrate.Open(ctx, url+"&search_path=other", migrate.Config{Source: testdata, Dir: "testdata/ok"})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Up(ctx); err != nil {
			t.Fatal(err)
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
