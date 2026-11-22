package pg_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pg"
	"github.com/dungnh3/kitchen/server"
)

// A service main: one logger, one pool, readiness wired into server.
func Example() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts, _ := log.OptionsFromEnv()
	logger, _ := log.Setup(opts)

	pool, err := pg.Open(ctx, pg.Config{
		URL:             os.Getenv("DATABASE_URL"),
		ApplicationName: "orders",
		RuntimeParams:   map[string]string{"timezone": "UTC"},
	}, pg.WithLogger(logger))
	if err != nil {
		logger.ErrorContext(ctx, "postgres", log.Err(err))
		return
	}
	defer pool.Close()

	srv, err := server.New(
		server.WithLogger(logger),
		server.WithServices( /* services that take pool */ ),
		server.WithReadiness(server.Check{Name: "postgres", Func: pool.Ping}),
	)
	if err != nil {
		logger.ErrorContext(ctx, "server", log.Err(err))
		return
	}
	if err := srv.Run(ctx); err != nil {
		logger.ErrorContext(ctx, "server", log.Err(err))
	}
}

func ExampleOpen() {
	ctx := context.Background()
	pool, err := pg.Open(ctx, pg.Config{
		URL:             os.Getenv("DATABASE_URL"),
		MaxConns:        50,
		ApplicationName: "orders",
		// Behind PgBouncer in transaction mode:
		// QueryExecMode: pg.ExecModeExec,
	}, pg.WithLogger(log.Default()))
	if err != nil {
		return
	}
	defer pool.Close()

	var n int
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&n)
}

// Fields instead of a URL. The password is escaped for you.
func ExampleOpen_fields() {
	ctx := context.Background()
	pool, err := pg.Open(ctx, pg.Config{
		Host:     "db.internal",
		User:     "orders",
		Password: os.Getenv("PG_PASSWORD"),
		Database: "orders",
		SSLMode:  "verify-full", // also checks the server certificate and host name
	})
	if err != nil {
		return
	}
	defer pool.Close()
}

// Local Docker Postgres without TLS. Never ship this: "disable" sends
// credentials and data in plaintext.
func ExampleOpen_localNoTLS() {
	ctx := context.Background()
	pool, err := pg.Open(ctx, pg.Config{
		Host:     "localhost",
		User:     "app",
		Password: os.Getenv("PG_PASSWORD"),
		Database: "app",
		SSLMode:  "disable", // or ?sslmode=disable inside a URL
	})
	if err != nil {
		return
	}
	defer pool.Close()
}

// Readiness: the pool's own Ping is a server.Check func.
func ExampleOpen_readiness() {
	ctx := context.Background()
	pool, err := pg.Open(ctx, pg.Config{URL: os.Getenv("DATABASE_URL")})
	if err != nil {
		return
	}
	defer pool.Close()
	_ = server.Check{Name: "postgres", Func: pool.Ping}
}

// DBTX is the interface sqlc generates for sql_package "pgx/v5".
// *pgxpool.Pool and pgx.Tx both satisfy it.
type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Queries stands in for sqlc's generated type (db.New(pool), q.WithTx(tx)).
type Queries struct{ db DBTX }

func New(db DBTX) *Queries { return &Queries{db: db} }

func (q *Queries) WithTx(tx pgx.Tx) *Queries { return &Queries{db: tx} }

func (q *Queries) MarkPaid(ctx context.Context, id int64) error {
	_, err := q.db.Exec(ctx, "UPDATE orders SET paid = true WHERE id = $1", id)
	return err
}

// sqlc on the pool, with a transaction and MapError at the repository edge.
func ExampleOpen_sqlc() {
	ctx := context.Background()
	pool, err := pg.Open(ctx, pg.Config{URL: os.Getenv("DATABASE_URL")})
	if err != nil {
		return
	}
	defer pool.Close()

	q := New(pool)
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		return q.WithTx(tx).MarkPaid(ctx, 42)
	})
	if err = pg.MapError(err); errors.Is(err, errs.ErrAborted) {
		// serialization failure or deadlock: retry the transaction
		return
	}
}

func ExampleMapError() {
	err := pg.MapError(&pgconn.PgError{Code: "23505", ConstraintName: "users_email_key"})
	fmt.Println(errors.Is(err, errs.ErrAlreadyExists), errs.Code(err))
	// Output: true AlreadyExists
}

// A repository maps errors once; handlers return them as they are.
func ExampleMapError_notFound() {
	err := pg.MapError(fmt.Errorf("get order 42: %w", pgx.ErrNoRows))
	fmt.Println(errors.Is(err, errs.ErrNotFound))
	fmt.Println(errs.Code(err))
	// Output:
	// true
	// NotFound
}

// ParseConfig plus OpenConfig, for pgx hooks such as AfterConnect.
func ExampleParseConfig() {
	ctx := context.Background()
	pc, err := pg.ParseConfig(pg.Config{
		URL:              os.Getenv("DATABASE_URL"),
		StatementTimeout: 10 * time.Second,
	})
	if err != nil {
		return
	}
	pc.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Exec(ctx, "SET search_path TO orders, public")
		return err
	}
	pool, err := pg.OpenConfig(ctx, pc)
	if err != nil {
		return
	}
	defer pool.Close()
}

// Short-lived pools (per tenant, tests) skip the pool statistics callbacks,
// which are never unregistered from the MeterProvider.
func ExampleWithoutPoolStats() {
	ctx := context.Background()
	pool, err := pg.Open(ctx, pg.Config{URL: os.Getenv("TENANT_DATABASE_URL")},
		pg.WithoutPoolStats())
	if err != nil {
		return
	}
	defer pool.Close()
}
