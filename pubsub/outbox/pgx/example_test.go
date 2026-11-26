package pgx_test

import (
	"context"

	jpgx "github.com/jackc/pgx/v5"

	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/pgx"
	"github.com/dungnh3/kitchen/pubsub"
	"github.com/dungnh3/kitchen/pubsub/memory"
	"github.com/dungnh3/kitchen/pubsub/outbox"
	outboxpgx "github.com/dungnh3/kitchen/pubsub/outbox/pgx"
)

func Example() {
	ctx := context.Background()
	logger := log.Default() // main calls log.Setup once
	// pgx.Open: pool sizing, tracing (otelpgx) and metrics, ping on open.
	pool, err := pgx.Open(ctx, pgx.Config{URL: "postgres://localhost/app"})
	if err != nil {
		logger.ErrorContext(ctx, "startup failed", log.Err(err))
		return
	}
	defer pool.Close()
	store, _ := outboxpgx.New(pool)

	// Business write and message in one transaction.
	err = jpgx.BeginFunc(ctx, pool, func(tx jpgx.Tx) error {
		if _, err := tx.Exec(ctx, "UPDATE accounts SET balance = balance - 10 WHERE id = 1"); err != nil {
			return err
		}
		return store.Enqueue(ctx, tx, "payments", &pubsub.Message{Key: "account-1", Data: []byte(`{"debit":10}`)})
	})
	if err != nil {
		logger.ErrorContext(ctx, "transfer failed", log.Err(err))
		return
	}

	var publisher pubsub.Publisher = memory.New() // e.g. gcp.NewPublisher(client)
	relay, _ := outbox.NewRelay(store, publisher)
	go func() { _ = store.Listen(ctx) }()
	_ = relay.Run(ctx)
}
