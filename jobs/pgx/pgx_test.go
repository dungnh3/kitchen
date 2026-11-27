package pgx_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	jpgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/dungnh3/kitchen/jobs"
	"github.com/dungnh3/kitchen/jobs/internal/drivertest"
	jobspgx "github.com/dungnh3/kitchen/jobs/pgx"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pgx"
)

// startPostgres returns the DSN of KITCHEN_TEST_PG_DSN, or of a disposable
// Postgres, or skips the test.
func startPostgres(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("KITCHEN_TEST_PG_DSN"); dsn != "" {
		return dsn
	}
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

var tableSeq atomic.Int64

func newDriver(ctx context.Context, t *testing.T, pool *pgxpool.Pool) *jobspgx.Driver {
	t.Helper()
	table := fmt.Sprintf("jobs_test_%d_%d", time.Now().UnixNano(), tableSeq.Add(1))
	d, err := jobspgx.New(pool, jobspgx.WithTable(table), jobspgx.WithLogger(log.Discard()))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CreateTable(ctx); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestPostgres(t *testing.T) {
	url := startPostgres(t)
	ctx := context.Background()
	pool, err := pgx.Open(ctx, pgx.Config{URL: url, MaxConns: 24})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	t.Run("Conformance", func(t *testing.T) {
		drivertest.Run(ctx, t, func(t *testing.T) jobs.Store { return newDriver(ctx, t, pool) })
	})

	t.Run("CreateTableIsIdempotent", func(t *testing.T) {
		d := newDriver(ctx, t, pool)
		if err := d.CreateTable(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("InsertTxFollowsTheTransaction", func(t *testing.T) {
		d := newDriver(ctx, t, pool)
		in := []jobs.NewJob{{Kind: "a", Queue: "default", Args: []byte(`{}`), Metadata: []byte(`{}`), Priority: 1, MaxAttempts: 3}}
		rollback := errors.New("rollback")
		var rolledBack int64
		err := jpgx.BeginFunc(ctx, pool, func(tx jpgx.Tx) error {
			res, err := d.InsertTx(ctx, tx, in)
			if err != nil {
				return err
			}
			rolledBack = res[0].ID
			return rollback
		})
		if !errors.Is(err, rollback) {
			t.Fatalf("err = %v", err)
		}
		if _, err := d.Get(ctx, rolledBack); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("a rolled-back job exists: %v", err)
		}
		var committed int64
		err = jpgx.BeginFunc(ctx, pool, func(tx jpgx.Tx) error {
			res, err := d.InsertTx(ctx, tx, in)
			committed = res[0].ID
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if r, err := d.Get(ctx, committed); err != nil || r.State != jobs.StateAvailable {
			t.Fatalf("committed job = %+v, %v", r, err)
		}
	})

	t.Run("TablesAreIndependent", func(t *testing.T) {
		a, b := newDriver(ctx, t, pool), newDriver(ctx, t, pool)
		in := []jobs.NewJob{{Kind: "a", Queue: "default", Args: []byte(`{}`), Metadata: []byte(`{}`), Priority: 1, MaxAttempts: 3}}
		if _, err := a.Insert(ctx, in); err != nil {
			t.Fatal(err)
		}
		if got, err := b.Claim(ctx, "default", []string{"a"}, 10, time.Minute); err != nil || len(got) != 0 {
			t.Fatalf("table b claimed table a's job: %v, %v", got, err)
		}
		if got, err := a.Claim(ctx, "default", []string{"a"}, 10, time.Minute); err != nil || len(got) != 1 {
			t.Fatalf("table a claimed %v, %v", got, err)
		}
	})

	t.Run("SchemaQualifiedTable", func(t *testing.T) {
		if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS jobs_schema"); err != nil {
			t.Fatal(err)
		}
		d, err := jobspgx.New(pool, jobspgx.WithTable("jobs_schema.emails_jobs"))
		if err != nil {
			t.Fatal(err)
		}
		if err := d.CreateTable(ctx); err != nil {
			t.Fatal(err)
		}
		in := []jobs.NewJob{{Kind: "a", Queue: "default", Args: []byte(`{}`), Metadata: []byte(`{}`), Priority: 1, MaxAttempts: 3}}
		if _, err := d.Insert(ctx, in); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("NotifyWakesTheListener", func(t *testing.T) {
		d := newDriver(ctx, t, pool)
		lctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- d.Listen(lctx) }()
		defer func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("Listen = %v", err)
			}
		}()
		waitWake(t, d) // the catch-up signal after LISTEN
		in := []jobs.NewJob{{Kind: "a", Queue: "default", Args: []byte(`{}`), Metadata: []byte(`{}`), Priority: 1, MaxAttempts: 3}}
		// Insert may run before LISTEN is in place; the catch-up signal then covers it.
		if _, err := d.Insert(ctx, in); err != nil {
			t.Fatal(err)
		}
		waitWake(t, d)
	})

	t.Run("ClientEndToEnd", func(t *testing.T) {
		d := newDriver(ctx, t, pool)
		reg := jobs.NewRegistry()
		got := make(chan string, 1)
		if err := jobs.Register(reg, jobs.WorkFunc[hello](func(_ context.Context, j *jobs.Job[hello]) error {
			got <- j.Args.Name
			return nil
		})); err != nil {
			t.Fatal(err)
		}
		c, err := jobs.NewClient[jpgx.Tx](d, jobs.WithRegistry(reg), jobs.WithLogger(log.Discard()), jobs.WithPollInterval(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		rctx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- c.Run(rctx) }()
		defer func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("Run = %v", err)
			}
		}()
		var id int64
		err = jpgx.BeginFunc(ctx, pool, func(tx jpgx.Tx) error {
			res, err := c.InsertTx(ctx, tx, hello{Name: "world"}, nil)
			id = res.ID
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		// The poll interval is an hour, so only NOTIFY can start the job in time.
		select {
		case name := <-got:
			if name != "world" {
				t.Fatalf("name = %q", name)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("the job did not run")
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			r, err := c.Get(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if r.State == jobs.StateCompleted {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("state = %s", r.State)
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
}

type hello struct {
	Name string `json:"name"`
}

func (hello) Kind() string { return "hello" }

func waitWake(t *testing.T, d *jobspgx.Driver) {
	t.Helper()
	select {
	case <-d.Wake():
	case <-time.After(10 * time.Second):
		t.Fatal("no wake-up")
	}
}

func TestNewErrors(t *testing.T) {
	if _, err := jobspgx.New(nil); err == nil {
		t.Fatal("nil pool accepted")
	}
	for _, name := range []string{"", "1abc", "a-b", "a b", `a"b`, "a.b.c", "a;drop table x"} {
		if _, err := jobspgx.New(&pgxpool.Pool{}, jobspgx.WithTable(name)); err == nil {
			t.Errorf("table name %q accepted", name)
		}
	}
}

// TestZenMigrationMatchesSchema keeps the migration that "zen new service
// -with jobs" writes equal to the driver's Schema().
func TestZenMigrationMatchesSchema(t *testing.T) {
	b, err := os.ReadFile("../../zen/templates/service/migrations/00003_create_jobs.sql.tmpl")
	if err != nil {
		t.Skipf("zen templates not found next to the module: %v", err)
	}
	d, err := jobspgx.New(&pgxpool.Pool{}) // Schema needs no connection
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), d.Schema()) {
		t.Fatalf("the zen migration template does not contain Driver.Schema():\n%s", d.Schema())
	}
}
