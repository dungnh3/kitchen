package pg_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/dungnh3/kitchen/pg"
)

func TestParseConfigFields(t *testing.T) {
	pc, err := pg.ParseConfig(pg.Config{ //nolint:gosec // test fixture.
		Host: "db.internal", User: "app", Password: "p@ss w'rd/?", Database: "orders",
		MaxConns: 20, MinConns: 2, StatementTimeout: 3 * time.Second, ApplicationName: "svc",
		RuntimeParams: map[string]string{"timezone": "UTC"}, QueryExecMode: pg.ExecModeExec,
	})
	if err != nil {
		t.Fatal(err)
	}
	cc := pc.ConnConfig
	if cc.Host != "db.internal" || cc.Port != 5432 || cc.User != "app" || cc.Password != "p@ss w'rd/?" || cc.Database != "orders" {
		t.Fatalf("conn config = %+v", cc.Config)
	}
	if pc.MaxConns != 20 || pc.MinConns != 2 || pc.MaxConnLifetime != 30*time.Minute || pc.HealthCheckPeriod != 30*time.Second {
		t.Fatalf("pool config = %+v", pc)
	}
	if cc.RuntimeParams["statement_timeout"] != "3000" || cc.RuntimeParams["application_name"] != "svc" || cc.RuntimeParams["timezone"] != "UTC" {
		t.Fatalf("runtime params = %v", cc.RuntimeParams)
	}
	if cc.DefaultQueryExecMode != pgx.QueryExecModeExec {
		t.Fatalf("exec mode = %v", cc.DefaultQueryExecMode)
	}
	if cc.TLSConfig == nil || len(cc.Fallbacks) == 0 {
		t.Fatal("sslmode=prefer should set TLS with a plaintext fallback")
	}
}

func TestParseConfigURLOverrides(t *testing.T) {
	pc, err := pg.ParseConfig(pg.Config{URL: "postgres://u:p@h1:6543/d1?sslmode=disable", Database: "d2", Password: "secret"}) //nolint:gosec // test fixture.
	if err != nil {
		t.Fatal(err)
	}
	if pc.ConnConfig.Host != "h1" || pc.ConnConfig.Port != 6543 || pc.ConnConfig.Database != "d2" || pc.ConnConfig.Password != "secret" {
		t.Fatalf("got %+v", pc.ConnConfig.Config)
	}
	if pc.ConnConfig.TLSConfig != nil {
		t.Fatal("sslmode=disable must not set TLS")
	}
}

func TestParseConfigErrors(t *testing.T) {
	bad := []pg.Config{
		{},
		{Host: "h"},
		{URL: "postgres://h/d", SSLMode: "require"},
		{Host: "h", Database: "d", MaxConns: 2, MinConns: 3},
		{Host: "h", Database: "d", QueryExecMode: "turbo"},
		{URL: "::not a url::"},
	}
	for i, cfg := range bad {
		if _, err := pg.ParseConfig(cfg); err == nil {
			t.Errorf("case %d: want error", i)
		}
	}
}

func TestOpenUnreachable(t *testing.T) {
	_, err := pg.Open(context.Background(), pg.Config{URL: "postgres://u:p@127.0.0.1:1/d?sslmode=disable", ConnectTimeout: 500 * time.Millisecond}) //nolint:gosec // test fixture.
	if err == nil || !strings.Contains(err.Error(), "pg:") {
		t.Fatalf("err = %v", err)
	}
}

// startPostgres starts a disposable Postgres or skips the test.
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

func TestOpenIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx := context.Background()
	pool, err := pg.Open(ctx, pg.Config{URL: url, MaxConns: 4, RuntimeParams: map[string]string{"timezone": "Asia/Ho_Chi_Minh"}})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pg.Ping(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var tz string
	if err := pool.QueryRow(ctx, "SHOW timezone").Scan(&tz); err != nil || tz != "Asia/Ho_Chi_Minh" {
		t.Fatalf("timezone = %q, %v", tz, err)
	}
}
