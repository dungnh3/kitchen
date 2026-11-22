package mysqlx_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/mysqlx"
	"github.com/dungnh3/kitchen/server"
)

func ExampleOpen() {
	ctx := context.Background()
	db, err := mysqlx.Open(ctx, mysqlx.Config{
		DSN:          os.Getenv("MYSQL_DSN"), // user:pass@tcp(host:3306)/db
		MaxOpenConns: 50,
	})
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()

	_ = server.Check{Name: "mysql", Func: db.PingContext} // readiness
	var n int
	_ = db.QueryRowContext(ctx, "SELECT count(*) FROM orders").Scan(&n)
}

func ExampleOpen_addr() {
	ctx := context.Background()
	db, err := mysqlx.Open(ctx, mysqlx.Config{
		Addr:     "mysql.internal", // port defaults to 3306
		User:     "orders",
		Password: os.Getenv("MYSQL_PASSWORD"),
		Database: "orders",
		TLSMode:  mysqlx.TLSVerify, // verify the certificate and host name
		Params:   map[string]string{"sql_mode": "'STRICT_ALL_TABLES'"},
	})
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
}

func Example_server() {
	ctx := context.Background()
	opts, _ := log.OptionsFromEnv()
	logger, _ := log.Setup(opts) // once, in main

	db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: os.Getenv("MYSQL_DSN")},
		mysqlx.WithLogger(logger))
	if err != nil {
		logger.ErrorContext(ctx, "mysql", log.Err(err))
		return
	}
	defer func() { _ = db.Close() }()

	srv, err := server.New(
		server.WithServices( /* your services, holding db */ ),
		server.WithReadiness(server.Check{Name: "mysql", Func: db.PingContext}),
	)
	if err != nil {
		logger.ErrorContext(ctx, "server", log.Err(err))
		return
	}
	_ = srv.Run(ctx)
}

func ExampleParseConfig() {
	mc, err := mysqlx.ParseConfig(mysqlx.Config{
		Addr:     "mysql.internal",
		User:     "orders",
		Database: "orders",
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(mc.Addr, mc.TLSConfig, mc.ParseTime, mc.Loc, mc.Timeout)
	fmt.Println(mc.MultiStatements, mc.InterpolateParams, mc.AllowAllFiles)
	// Output:
	// mysql.internal:3306 skip-verify true UTC 5s
	// false false false
}

func ExampleOpenConfig() {
	ctx := context.Background()
	mc, err := mysqlx.ParseConfig(mysqlx.Config{Addr: "mysql.internal", User: "orders", Database: "orders"})
	if err != nil {
		return
	}
	// Fetch a short-lived password (cloud IAM token, Vault lease) per connection.
	err = mc.Apply(mysql.BeforeConnect(func(_ context.Context, c *mysql.Config) error {
		c.Passwd = os.Getenv("MYSQL_TOKEN") // replace with your token source
		return nil
	}))
	if err != nil {
		return
	}
	db, err := mysqlx.OpenConfig(ctx, mc)
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
	// OpenConfig keeps database/sql's pool defaults: set the limits yourself.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(10 * time.Minute) // below the token lifetime
}

func ExampleConfig_customCA() {
	pem, err := os.ReadFile("/etc/mysql/ca.pem") // your provider's CA bundle
	if err != nil {
		return
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pem)
	db, err := mysqlx.Open(context.Background(), mysqlx.Config{
		Addr: "mysql.internal", User: "orders", Database: "orders",
		// Verifies the certificate against roots and the host name
		// (ServerName defaults to the Addr host).
		TLS: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
	})
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
}

func ExampleTLSDisable() {
	// Local docker-compose only: the password exchange and all data travel
	// in plaintext. Never use it for a server reached over a network.
	db, err := mysqlx.Open(context.Background(), mysqlx.Config{
		DSN: "app:app@tcp(localhost:3306)/app?tls=false",
	})
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
}

func ExampleMapError() {
	dup := fmt.Errorf("insert user: %w", &mysql.MySQLError{
		Number: 1062, Message: "Duplicate entry 'a@b.c' for key 'users.email'",
	})
	err := mysqlx.MapError(dup)
	fmt.Println(errors.Is(err, errs.ErrAlreadyExists), errs.Code(err))

	err = mysqlx.MapError(fmt.Errorf("get user: %w", sql.ErrNoRows))
	fmt.Println(errors.Is(err, errs.ErrNotFound), errs.HTTPStatus(err))
	// Output:
	// true AlreadyExists
	// true 404
}

func ExampleMapError_retry() {
	ctx := context.Background()
	var db *sql.DB // from mysqlx.Open
	transfer := func(ctx context.Context) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return mysqlx.MapError(err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - 10 WHERE id = ?", 1); err != nil {
			return mysqlx.MapError(err)
		}
		if _, err := tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + 10 WHERE id = ?", 2); err != nil {
			return mysqlx.MapError(err)
		}
		return mysqlx.MapError(tx.Commit())
	}
	for range 3 {
		// Deadlock (1213) and lock wait timeout (1205) map to ErrAborted.
		if err := transfer(ctx); !errors.Is(err, errs.ErrAborted) {
			break
		}
	}
}

func ExampleWithoutPoolStats() {
	// Per-tenant pools that are opened and closed at runtime: skip the
	// pool-stats callbacks, which would otherwise never be unregistered.
	openTenant := func(ctx context.Context, dsn string) (*sql.DB, error) {
		return mysqlx.Open(ctx, mysqlx.Config{DSN: dsn, MaxOpenConns: 5}, mysqlx.WithoutPoolStats())
	}
	db, err := openTenant(context.Background(), os.Getenv("TENANT_DSN"))
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
}
