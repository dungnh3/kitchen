package mysqlx_test

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/mysqlx"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestParseConfigFields(t *testing.T) {
	mc, err := mysqlx.ParseConfig(mysqlx.Config{ //nolint:gosec // test fixture.
		Addr: "db.internal", User: "app", Password: "p@ss w'rd/?", Database: "orders",
		ReadTimeout: 3 * time.Second, Params: map[string]string{"sql_mode": "'STRICT_ALL_TABLES'"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mc.Addr != "db.internal:3306" || mc.User != "app" || mc.Passwd != "p@ss w'rd/?" || mc.DBName != "orders" {
		t.Fatalf("config = %+v", mc)
	}
	if mc.TLSConfig != mysqlx.TLSRequire || !mc.ParseTime || mc.Loc != time.UTC || mc.Timeout != 5*time.Second || mc.ReadTimeout != 3*time.Second {
		t.Fatalf("defaults = tls %q parseTime %v loc %v timeout %v", mc.TLSConfig, mc.ParseTime, mc.Loc, mc.Timeout)
	}
	if mc.MultiStatements || mc.InterpolateParams || mc.AllowAllFiles {
		t.Fatal("unsafe driver options must stay off")
	}
	if mc.Params["sql_mode"] != "'STRICT_ALL_TABLES'" {
		t.Fatalf("params = %v", mc.Params)
	}
}

func TestParseConfigDSN(t *testing.T) {
	mc, err := mysqlx.ParseConfig(mysqlx.Config{DSN: "u:p@tcp(h1:3307)/d1", Database: "d2", Password: "secret"}) //nolint:gosec // test fixture.
	if err != nil {
		t.Fatal(err)
	}
	if mc.Addr != "h1:3307" || mc.DBName != "d2" || mc.Passwd != "secret" || mc.TLSConfig != mysqlx.TLSRequire {
		t.Fatalf("got addr %q db %q tls %q", mc.Addr, mc.DBName, mc.TLSConfig)
	}
	mc, err = mysqlx.ParseConfig(mysqlx.Config{DSN: "u:p@tcp(h1)/d1?tls=false"})
	if err != nil || mc.TLSConfig != "false" {
		t.Fatalf("explicit tls=false not kept: %v %q", err, mc.TLSConfig)
	}
}

func TestParseConfigErrors(t *testing.T) {
	for i, cfg := range []mysqlx.Config{
		{},
		{Addr: "h"},
		{DSN: "u:p@tcp(h)/d", Addr: "other:3306"},
		{DSN: "u:p@tcp(h)/d", TLSMode: mysqlx.TLSDisable},
		{Addr: "h", Database: "d", TLSMode: "no-such-config"},
		{DSN: "not a dsn"},
		{Addr: "h", Database: "d", TLS: &tls.Config{}, TLSMode: mysqlx.TLSVerify},
		{DSN: "u:p@tcp(h)/d?tls=true", TLS: &tls.Config{}},
	} {
		if _, err := mysqlx.ParseConfig(cfg); err == nil {
			t.Errorf("case %d: want error", i)
		}
	}
}

func TestParseConfigCustomTLS(t *testing.T) {
	own := &tls.Config{MinVersion: tls.VersionTLS12}
	for _, cfg := range []mysqlx.Config{
		{Addr: "db.internal", Database: "d", TLS: own},
		{DSN: "u:p@tcp(db.internal:3306)/d", TLS: own},
	} {
		mc, err := mysqlx.ParseConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if mc.TLS == nil || mc.TLS == own || mc.TLS.ServerName != "db.internal" {
			t.Fatalf("TLS = %+v, want a copy verifying db.internal", mc.TLS)
		}
	}
	if own.ServerName != "" {
		t.Fatal("caller's tls.Config was modified")
	}
}

func TestOpenUnreachable(t *testing.T) {
	_, err := mysqlx.Open(context.Background(), mysqlx.Config{
		Addr: "127.0.0.1:1", Database: "d", TLSMode: mysqlx.TLSDisable, ConnectTimeout: 500 * time.Millisecond,
	}, mysqlx.WithoutPoolStats())
	if err == nil || !strings.Contains(err.Error(), "mysqlx:") {
		t.Fatalf("err = %v", err)
	}
}

func TestMapError(t *testing.T) {
	my := func(n uint16) error {
		return fmt.Errorf("insert: %w", &mysql.MySQLError{Number: n, Message: "Duplicate entry 'a@b' for key 'users.email'"})
	}
	for _, tt := range []struct {
		in   error
		want *errs.Error
	}{
		{fmt.Errorf("get: %w", sql.ErrNoRows), errs.ErrNotFound},
		{my(1062), errs.ErrAlreadyExists},
		{my(1452), errs.ErrFailedPrecondition},
		{my(3819), errs.ErrFailedPrecondition},
		{my(1406), errs.ErrInvalidArgument},
		{my(1213), errs.ErrAborted},
		{my(1205), errs.ErrAborted},
		{my(3024), errs.ErrDeadlineExceeded},
		{my(1040), errs.ErrUnavailable},
		{mysql.ErrInvalidConn, errs.ErrUnavailable},
	} {
		got := mysqlx.MapError(tt.in)
		if !errors.Is(got, tt.want) {
			t.Errorf("%v → %v, want %v", tt.in, got, tt.want)
		}
	}
	for _, in := range []error{my(1064), context.Canceled, errors.New("boom"), errs.ErrInternal.With()} {
		if got := mysqlx.MapError(in); got != in { //nolint:errorlint // identity: unchanged.
			t.Errorf("%v changed to %v", in, got)
		}
	}
	st, _ := errs.ToStatus(mysqlx.MapError(my(1062)))
	if strings.Contains(st.Message(), "users.email") {
		t.Fatalf("key name leaked: %q", st.Message())
	}
}

func TestOpenIntegration(t *testing.T) {
	if testing.Short() || os.Getenv("KITCHEN_SKIP_DOCKER") != "" {
		t.Skip("docker tests disabled")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	c, err := tcmysql.Run(ctx, "mysql:8.4", tcmysql.WithDatabase("app"), tcmysql.WithUsername("app"), tcmysql.WithPassword("app"))
	testcontainers.CleanupContainer(t, c)
	if err != nil {
		t.Fatal(err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatal(err)
	}
	// Default TLS (encrypted, unverified) works against MySQL's self-signed cert.
	db, err := mysqlx.Open(ctx, mysqlx.Config{Addr: host + ":" + port.Port(), User: "app", Password: "app", Database: "app"}, mysqlx.WithoutPoolStats())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var cipher, ignored string
	if err := db.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&ignored, &cipher); err != nil || cipher == "" {
		t.Fatalf("connection not encrypted: cipher %q, %v", cipher, err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE u (email VARCHAR(64) PRIMARY KEY, at DATETIME)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO u VALUES (?, ?)", "a@b", time.Now()); err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, "INSERT INTO u VALUES (?, ?)", "a@b", time.Now())
	if !errors.Is(mysqlx.MapError(err), errs.ErrAlreadyExists) {
		t.Fatalf("duplicate: %v", err)
	}
	var at time.Time // parseTime is on
	if err := db.QueryRowContext(ctx, "SELECT at FROM u").Scan(&at); err != nil {
		t.Fatal(err)
	}
	err = db.QueryRowContext(ctx, "SELECT at FROM u WHERE email = ?", "none").Scan(&at)
	if !errors.Is(mysqlx.MapError(err), errs.ErrNotFound) {
		t.Fatalf("no rows: %v", err)
	}
}
