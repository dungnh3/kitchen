package mysql_test

import (
	"context"
	"database/sql"

	"github.com/dungnh3/kitchen/jobs"
	jobsmysql "github.com/dungnh3/kitchen/jobs/mysql"
	"github.com/dungnh3/kitchen/mysqlx"
)

type SendWelcome struct {
	UserID string `json:"user_id"`
}

func (SendWelcome) Kind() string { return "send_welcome" }

func Example() {
	ctx := context.Background()
	db, err := mysqlx.Open(ctx, mysqlx.Config{Addr: "db.internal:3306", User: "app", Password: "secret", Database: "app"})
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close() }()

	// The table comes from a migration: jobsmysql.New(db).Schema() is its SQL.
	drv, err := jobsmysql.New(db, jobsmysql.WithTable("emails_jobs"))
	if err != nil {
		panic(err)
	}
	client, err := jobs.NewClient[*sql.Tx](drv)
	if err != nil {
		panic(err)
	}

	// The job is committed with the business write, or not at all.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		panic(err)
	}
	defer func() { _ = tx.Rollback() }()
	// _, err = tx.ExecContext(ctx, "INSERT INTO users ...")
	if _, err := client.InsertTx(ctx, tx, SendWelcome{UserID: "u-42"}, nil); err != nil {
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}
}
