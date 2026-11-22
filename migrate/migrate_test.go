package migrate_test

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/migrate"
	"github.com/dungnh3/kitchen/mysqlx"
)

//go:embed testdata
var testdata embed.FS

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestValidation(t *testing.T) {
	if _, err := migrate.NewPostgres(nil, migrate.Config{Source: testdata}); err == nil {
		t.Fatal("nil pool: want error")
	}
	if _, err := migrate.NewMySQL(nil, migrate.Config{Source: testdata}); err == nil {
		t.Fatal("nil db: want error")
	}
	if _, err := migrate.Open(context.Background(), "sqlite", "x", migrate.Config{Source: testdata}); err == nil {
		t.Fatal("unknown dialect: want error")
	}
}

func docker(t *testing.T) {
	t.Helper()
	if testing.Short() || os.Getenv("KITCHEN_SKIP_DOCKER") != "" {
		t.Skip("docker tests disabled")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
}

func startPostgres(t *testing.T) string {
	t.Helper()
	docker(t)
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

func version(ctx context.Context, t *testing.T, m *migrate.Migrator) int64 {
	t.Helper()
	v, err := m.Version(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestPostgres(t *testing.T) {
	url := startPostgres(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	t.Run("up down to keep pool open", func(t *testing.T) {
		m, err := migrate.NewPostgres(pool, migrate.Config{Source: testdata, Dir: "testdata/pg"})
		if err != nil {
			t.Fatal(err)
		}
		if v := version(ctx, t, m); v != 0 {
			t.Fatalf("initial version = %d", v)
		}
		if pending, err := m.HasPending(ctx); err != nil || !pending {
			t.Fatalf("pending = %v, %v", pending, err)
		}
		if err := m.Up(ctx); err != nil {
			t.Fatal(err)
		}
		if err := m.Up(ctx); err != nil { // nothing pending is not an error
			t.Fatal(err)
		}
		if v := version(ctx, t, m); v != 20250313100000 {
			t.Fatalf("after Up v = %d", v)
		}
		if err := m.Down(ctx, 1); err != nil { // exactly one step
			t.Fatal(err)
		}
		if v := version(ctx, t, m); v != 20250304100000 {
			t.Fatalf("after Down(1) v = %d", v)
		}
		if err := m.Down(ctx, 0); err == nil {
			t.Fatal("Down(0): want error")
		}
		if err := m.To(ctx, 0); err != nil { // revert everything
			t.Fatal(err)
		}
		if v := version(ctx, t, m); v != 0 {
			t.Fatalf("after To(0) v = %d", v)
		}
		if err := m.Down(ctx, 5); err != nil { // nothing left: not an error
			t.Fatal(err)
		}
		if err := m.To(ctx, 20250313100000); err != nil {
			t.Fatal(err)
		}
		st, err := m.Status(ctx)
		if err != nil || len(st) != 3 || st[2].State != migrate.StateApplied {
			t.Fatalf("status = %v, %v", st, err)
		}
		if err := m.Close(); err != nil {
			t.Fatal(err)
		}
		var n int // the caller's pool is still usable after Close
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM items").Scan(&n); err != nil || n != 1 {
			t.Fatalf("pool after Close: n=%d err=%v", n, err)
		}
	})

	t.Run("failed migration rolls back, no dirty state", func(t *testing.T) {
		m, err := migrate.NewPostgres(pool, migrate.Config{Source: testdata, Dir: "testdata/broken", Table: "broken_version"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = m.Close() }()
		if err := m.Up(ctx); err == nil {
			t.Fatal("broken Up: want error")
		}
		if v := version(ctx, t, m); v != 1 {
			t.Fatalf("version = %d, want 1", v)
		}
		var b *string // table b was created in the failed migration's tx
		if err := pool.QueryRow(ctx, "SELECT to_regclass('b')::text").Scan(&b); err != nil || b != nil {
			t.Fatalf("table b = %v, %v (want rolled back)", b, err)
		}
	})

	t.Run("concurrent runs are serialised", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "CREATE SCHEMA race"); err != nil {
			t.Fatal(err)
		}
		racePool, err := pgxpool.New(ctx, url+"&search_path=race")
		if err != nil {
			t.Fatal(err)
		}
		defer racePool.Close()
		var wg sync.WaitGroup
		errs := make([]error, 4)
		for i := range errs {
			wg.Go(func() {
				m, err := migrate.NewPostgres(racePool, migrate.Config{Source: testdata, Dir: "testdata/pg"})
				if err != nil {
					errs[i] = err
					return
				}
				defer func() { _ = m.Close() }()
				errs[i] = m.Up(ctx)
			})
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			t.Fatal(err)
		}
		var n int // every migration applied exactly once
		if err := racePool.QueryRow(ctx, "SELECT count(*) FROM goose_db_version WHERE version_id > 0").Scan(&n); err != nil || n != 3 {
			t.Fatalf("applied rows = %d, %v", n, err)
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		m, err := migrate.NewPostgres(pool, migrate.Config{Source: testdata, Dir: "testdata/pg", Table: "cancel_version"})
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

	t.Run("Go migrations", func(t *testing.T) {
		up := func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "CREATE TABLE go_made (id int)")
			return err
		}
		down := func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "DROP TABLE go_made")
			return err
		}
		m, err := migrate.NewPostgres(pool, migrate.Config{GoMigrations: []*migrate.GoMigration{migrate.NewGoMigration(1, up, down)}, Table: "go_version"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = m.Close() }()
		if err := m.Up(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, "SELECT 1 FROM go_made"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Open owns its pool", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "CREATE SCHEMA other"); err != nil {
			t.Fatal(err)
		}
		m, err := migrate.Open(ctx, migrate.Postgres, url+"&search_path=other", migrate.Config{Source: testdata, Dir: "testdata/pg"})
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

func TestMySQL(t *testing.T) {
	docker(t)
	ctx := context.Background()
	c, err := tcmysql.Run(ctx, "mysql:8.4", tcmysql.WithDatabase("app"), tcmysql.WithUsername("app"), tcmysql.WithPassword("app"))
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatal(err)
	}
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "3306/tcp")
	dsn := "app:app@tcp(" + host + ":" + port.Port() + ")/app"
	db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: dsn}, mysqlx.WithoutPoolStats())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	m, err := migrate.NewMySQL(db, migrate.Config{Source: testdata, Dir: "testdata/mysql"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if v := version(ctx, t, m); v != 20250313100000 {
		t.Fatalf("after Up v = %d", v)
	}
	if err := m.Down(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if v := version(ctx, t, m); v != 20250304001122 {
		t.Fatalf("after Down(2) v = %d", v)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.PingContext(ctx); err != nil { // caller's db stays open
		t.Fatal(err)
	}

	m, err = migrate.Open(ctx, migrate.MySQL, dsn, migrate.Config{Source: testdata, Dir: "testdata/mysql"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}
