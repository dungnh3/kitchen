package mysql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	"go.uber.org/goleak"

	"github.com/dungnh3/kitchen/jobs"
	"github.com/dungnh3/kitchen/jobs/internal/drivertest"
	jobsmysql "github.com/dungnh3/kitchen/jobs/mysql"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/mysqlx"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// startMySQL returns a connection config for KITCHEN_TEST_MYSQL_DSN, or for
// a disposable MySQL, or skips the test.
func startMySQL(t *testing.T) mysqlx.Config {
	t.Helper()
	if dsn := os.Getenv("KITCHEN_TEST_MYSQL_DSN"); dsn != "" {
		return mysqlx.Config{DSN: dsn}
	}
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
	return mysqlx.Config{Addr: host + ":" + port.Port(), User: "app", Password: "app", Database: "app", MaxOpenConns: 24}
}

var tableSeq atomic.Int64

func newDriver(ctx context.Context, t *testing.T, db *sql.DB) *jobsmysql.Driver {
	t.Helper()
	table := fmt.Sprintf("jobs_test_%d_%d", time.Now().UnixNano(), tableSeq.Add(1))
	d, err := jobsmysql.New(db, jobsmysql.WithTable(table))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.CreateTable(ctx); err != nil {
		t.Fatal(err)
	}
	return d
}

func newJob() jobs.NewJob {
	return jobs.NewJob{Kind: "a", Queue: "default", Args: []byte(`{}`), Metadata: []byte(`{}`), Priority: 1, MaxAttempts: 3}
}

func TestMySQL(t *testing.T) {
	cfg := startMySQL(t)
	ctx := context.Background()
	db, err := mysqlx.Open(ctx, cfg, mysqlx.WithoutPoolStats())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	t.Run("Conformance", func(t *testing.T) {
		drivertest.Run(ctx, t, func(t *testing.T) jobs.Store { return newDriver(ctx, t, db) })
	})

	t.Run("CreateTableIsIdempotent", func(t *testing.T) {
		if err := newDriver(ctx, t, db).CreateTable(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("InsertTxFollowsTheTransaction", func(t *testing.T) {
		d := newDriver(ctx, t, db)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		res, err := d.InsertTx(ctx, tx, []jobs.NewJob{newJob()})
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		if _, err := d.Get(ctx, res[0].ID); !errors.Is(err, jobs.ErrNotFound) {
			t.Fatalf("a rolled-back job exists: %v", err)
		}
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		res, err = d.InsertTx(ctx, tx, []jobs.NewJob{newJob()})
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if r, err := d.Get(ctx, res[0].ID); err != nil || r.State != jobs.StateAvailable {
			t.Fatalf("committed job = %+v, %v", r, err)
		}
	})

	t.Run("TablesAreIndependent", func(t *testing.T) {
		a, b := newDriver(ctx, t, db), newDriver(ctx, t, db)
		if _, err := a.Insert(ctx, []jobs.NewJob{newJob()}); err != nil {
			t.Fatal(err)
		}
		if got, err := b.Claim(ctx, "default", []string{"a"}, 10, time.Minute); err != nil || len(got) != 0 {
			t.Fatalf("table b claimed table a's job: %v, %v", got, err)
		}
		if got, err := a.Claim(ctx, "default", []string{"a"}, 10, time.Minute); err != nil || len(got) != 1 {
			t.Fatalf("table a claimed %v, %v", got, err)
		}
	})

	t.Run("ClientEndToEnd", func(t *testing.T) {
		d := newDriver(ctx, t, db)
		reg := jobs.NewRegistry()
		got := make(chan string, 1)
		if err := jobs.Register(reg, jobs.WorkFunc[hello](func(_ context.Context, j *jobs.Job[hello]) error {
			got <- j.Args.Name
			return nil
		})); err != nil {
			t.Fatal(err)
		}
		c, err := jobs.NewClient[*sql.Tx](d, jobs.WithRegistry(reg), jobs.WithLogger(log.Discard()), jobs.WithPollInterval(50*time.Millisecond))
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
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		res, err := c.InsertTx(ctx, tx, hello{Name: "world"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
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
			r, err := c.Get(ctx, res.ID)
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

func TestNewErrors(t *testing.T) {
	if _, err := jobsmysql.New(nil); err == nil {
		t.Fatal("nil db accepted")
	}
	for _, name := range []string{"", "1abc", "a-b", "a b", "a`b", "a.b.c", "a;drop table x"} {
		if _, err := jobsmysql.New(&sql.DB{}, jobsmysql.WithTable(name)); err == nil {
			t.Errorf("table name %q accepted", name)
		}
	}
}
