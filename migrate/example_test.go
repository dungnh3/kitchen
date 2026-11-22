package migrate_test

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/migrate"
	"github.com/dungnh3/kitchen/mysqlx"
	"github.com/dungnh3/kitchen/pg"
	"github.com/dungnh3/kitchen/server"
)

//go:embed testdata/pg/*.sql
var migrations embed.FS

//go:embed testdata/mysql/*.sql
var mysqlMigrations embed.FS

func ExampleNewPostgres() {
	ctx := context.Background()
	pool, err := pg.Open(ctx, pg.Config{URL: os.Getenv("DATABASE_URL")})
	if err != nil {
		return
	}
	defer pool.Close()

	logger := log.Default() // the process logger main installed with log.Setup
	m, err := migrate.NewPostgres(pool, migrate.Config{Source: migrations, Dir: "testdata/pg", Logger: logger})
	if err != nil {
		return
	}
	defer func() { _ = m.Close() }() // does not close pool

	if err := m.Up(ctx); err != nil {
		logger.ErrorContext(ctx, "migrations failed; the failed one was rolled back", log.Err(err))
	}
}

// Example runs migrations at service startup, before the server serves.
func Example() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts, err := log.OptionsFromEnv()
	logger, _ := log.Setup(opts)
	if err != nil {
		logger.WarnContext(ctx, "invalid log config", log.Err(err))
	}

	pool, err := pg.Open(ctx, pg.Config{URL: os.Getenv("DATABASE_URL")}, pg.WithLogger(logger))
	if err != nil {
		logger.ErrorContext(ctx, "postgres", log.Err(err))
		return
	}
	defer pool.Close()

	// Several pods may start at once: they wait on the migration lock, and
	// only the first one applies anything.
	m, err := migrate.NewPostgres(pool, migrate.Config{Source: migrations, Dir: "testdata/pg", Logger: logger})
	if err != nil {
		logger.ErrorContext(ctx, "migrate", log.Err(err))
		return
	}
	err = m.Up(ctx)
	_ = m.Close() // releases the migrator; pool stays open for the service
	if err != nil {
		logger.ErrorContext(ctx, "migrate", log.Err(err))
		return
	}

	srv, err := server.New(
		server.WithServices( /* your services, built on pool */ ),
		server.WithAdmin(":9090"),
	)
	if err != nil {
		logger.ErrorContext(ctx, "server", log.Err(err))
		return
	}
	if err := srv.Run(ctx); err != nil {
		logger.ErrorContext(ctx, "server", log.Err(err))
	}
}

func ExampleNewMySQL() {
	ctx := context.Background()
	logger := log.Default()
	db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: os.Getenv("MYSQL_DSN")}, mysqlx.WithLogger(logger))
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()

	m, err := migrate.NewMySQL(db, migrate.Config{Source: mysqlMigrations, Dir: "testdata/mysql", Logger: logger})
	if err != nil {
		return
	}
	defer func() { _ = m.Close() }() // does not close db

	if err := m.Up(ctx); err != nil {
		logger.ErrorContext(ctx, "migrate", log.Err(err))
	}
}

// ExampleOpen is a CI or deploy gate: fail when the database is behind.
func ExampleOpen() {
	ctx := context.Background()
	logger := log.Default()
	m, err := migrate.Open(ctx, migrate.Postgres, os.Getenv("DATABASE_URL"),
		migrate.Config{Source: migrations, Dir: "testdata/pg", Logger: logger})
	if err != nil {
		logger.ErrorContext(ctx, "migrate", log.Err(err))
		return
	}
	defer func() { _ = m.Close() }() // closes the connection Open made

	pending, err := m.HasPending(ctx) // takes no lock
	if err != nil {
		logger.ErrorContext(ctx, "migrate", log.Err(err))
		return
	}
	if pending {
		logger.ErrorContext(ctx, "database has pending migrations")
	}
}

// ExampleMigrator_HasPending gates readiness on migrations that a separate
// job applies.
func ExampleMigrator_HasPending() {
	ctx := context.Background()
	logger := log.Default()
	pool, err := pg.Open(ctx, pg.Config{URL: os.Getenv("DATABASE_URL")}, pg.WithLogger(logger))
	if err != nil {
		return
	}
	defer pool.Close()

	m, err := migrate.NewPostgres(pool, migrate.Config{Source: migrations, Dir: "testdata/pg", Logger: logger})
	if err != nil {
		return
	}
	defer func() { _ = m.Close() }()

	migrated := server.Check{Name: "migrations", Func: func(ctx context.Context) error {
		pending, err := m.HasPending(ctx)
		if err != nil {
			return err
		}
		if pending {
			return errors.New("migrations pending")
		}
		return nil
	}}
	srv, err := server.New(
		server.WithServices( /* your services */ ),
		server.WithAdmin(":9090", server.WithAdminChecks(migrated)),
	)
	if err != nil {
		return
	}
	_ = srv.Run(ctx)
}

func ExampleMigrator_Status() {
	ctx := context.Background()
	m, err := migrate.Open(ctx, migrate.Postgres, os.Getenv("DATABASE_URL"),
		migrate.Config{Source: migrations, Dir: "testdata/pg"})
	if err != nil {
		return
	}
	defer func() { _ = m.Close() }()

	st, err := m.Status(ctx)
	if err != nil {
		return
	}
	for _, s := range st {
		fmt.Println(s.Source.Version, s.State, s.AppliedAt) // AppliedAt is zero when pending
	}
}

func ExampleMigrator_Down() {
	ctx := context.Background()
	m, err := migrate.Open(ctx, migrate.Postgres, os.Getenv("DATABASE_URL"),
		migrate.Config{Source: migrations, Dir: "testdata/pg"})
	if err != nil {
		return
	}
	defer func() { _ = m.Close() }()

	_ = m.Down(ctx, 1)            // revert the newest migration
	_ = m.To(ctx, 20250304100000) // up or down to exactly this version
	v, _ := m.Version(ctx)        // 20250304100000
	_ = m.To(ctx, 0)              // revert everything
	fmt.Println("was at version", v)
}

// ExampleNewPostgres_goMigrations runs a migration written in Go. Use it
// for data changes that SQL cannot express.
func ExampleNewPostgres_goMigrations() {
	ctx := context.Background()
	pool, err := pg.Open(ctx, pg.Config{URL: os.Getenv("DATABASE_URL")})
	if err != nil {
		return
	}
	defer pool.Close()

	up := func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "UPDATE items SET name = lower(name)")
		return err
	}
	m, err := migrate.NewPostgres(pool, migrate.Config{
		Source:       migrations,
		Dir:          "testdata/pg",
		GoMigrations: []*migrate.GoMigration{migrate.NewGoMigration(20250320000000, up, nil)},
	})
	if err != nil {
		return
	}
	defer func() { _ = m.Close() }()
	_ = m.Up(ctx) // SQL and Go migrations run in one version order
}

func ExampleConfig() {
	cfg := migrate.Config{
		Source:          migrations,
		Dir:             "testdata/pg",
		Table:           "billing_schema_version", // one history per module sharing a database
		LockTimeout:     5 * time.Minute,          // a long migration holds the lock
		AllowOutOfOrder: true,                     // apply older versions merged late
		Logger:          log.Default(),
	}
	_ = cfg
}
