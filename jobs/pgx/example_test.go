package pgx_test

import (
	"context"

	jpgx "github.com/jackc/pgx/v5"

	"github.com/dungnh3/kitchen/jobs"
	jobspgx "github.com/dungnh3/kitchen/jobs/pgx"
	"github.com/dungnh3/kitchen/pgx"
)

type SendWelcome struct {
	UserID string `json:"user_id"`
}

func (SendWelcome) Kind() string { return "send_welcome" }

func Example() {
	ctx := context.Background()
	pool, err := pgx.Open(ctx, pgx.Config{URL: "postgres://app:secret@db.internal:5432/app"}) //nolint:gosec // example fixture.
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	// The table comes from a migration: jobspgx.New(pool).Schema() is its SQL.
	// A second driver on another table is a second, independent queue set.
	drv, err := jobspgx.New(pool, jobspgx.WithTable("emails_jobs"))
	if err != nil {
		panic(err)
	}
	client, err := jobs.NewClient[jpgx.Tx](drv)
	if err != nil {
		panic(err)
	}

	// The job is committed with the business write, or not at all.
	err = jpgx.BeginFunc(ctx, pool, func(tx jpgx.Tx) error {
		// _, err := tx.Exec(ctx, "INSERT INTO users ...")
		_, err := client.InsertTx(ctx, tx, SendWelcome{UserID: "u-42"}, nil)
		return err
	})
	if err != nil {
		panic(err)
	}
}
