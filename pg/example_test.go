package pg_test

import (
	"context"
	"log/slog"
	"os"

	"github.com/dungnh3/kitchen/pg"
)

func ExampleOpen() {
	ctx := context.Background()
	pool, err := pg.Open(ctx, pg.Config{
		URL:             os.Getenv("DATABASE_URL"),
		MaxConns:        50,
		ApplicationName: "orders",
		// Behind PgBouncer in transaction mode:
		// QueryExecMode: pg.ExecModeExec,
	}, pg.WithLogger(slog.Default()))
	if err != nil {
		return
	}
	defer pool.Close()

	var n int
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&n)
}
