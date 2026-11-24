package mongox_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/dungnh3/kitchen/errs"
	"github.com/dungnh3/kitchen/log"
	"github.com/dungnh3/kitchen/mongox"
	"github.com/dungnh3/kitchen/server"
)

func ExampleOpen() {
	ctx := context.Background()
	client, err := mongox.Open(ctx, mongox.Config{
		URI:     os.Getenv("MONGO_URI"), // mongodb://user:pass@db-0:27017,db-1:27017/?replicaSet=rs0
		AppName: "orders",
	})
	if err != nil {
		return
	}
	defer func() { _ = client.Disconnect(context.WithoutCancel(ctx)) }()

	var doc bson.M
	err = client.Database("app").Collection("orders").
		FindOne(ctx, bson.D{{Key: "_id", Value: "o1"}}).Decode(&doc)
	if errors.Is(mongox.MapError(err), errs.ErrNotFound) {
		return // no such order
	}
}

// Config fields with their defaults spelled out, plus an operation Timeout
// (default none).
func ExampleOpen_config() {
	ctx := context.Background()
	client, err := mongox.Open(ctx, mongox.Config{
		URI:                    os.Getenv("MONGO_URI"),
		AppName:                "orders",
		MaxPoolSize:            100,              // per server (driver default)
		MinPoolSize:            0,                // keep no warm connections
		MaxConnIdleTime:        5 * time.Minute,  // close idle connections
		ConnectTimeout:         5 * time.Second,  // TCP + TLS handshake
		ServerSelectionTimeout: 5 * time.Second,  // also bounds the ping in Open
		Timeout:                10 * time.Second, // only when ctx has no deadline; 0 = none
	})
	if err != nil {
		return
	}
	defer func() { _ = client.Disconnect(context.WithoutCancel(ctx)) }()
}

// A service main: one logger from log.Setup, passed to mongox, and the
// client's ping as a readiness check.
func ExampleOpen_server() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts, _ := log.OptionsFromEnv()
	logger, _ := log.Setup(opts)

	client, err := mongox.Open(ctx, mongox.Config{URI: os.Getenv("MONGO_URI"), AppName: "orders"},
		mongox.WithLogger(logger))
	if err != nil {
		logger.ErrorContext(ctx, "mongo", log.Err(err))
		return
	}
	defer func() { _ = client.Disconnect(context.WithoutCancel(ctx)) }()

	srv, err := server.New(
		server.WithLogger(logger),
		server.WithServices( /* your services, holding client */ ),
		server.WithReadiness(server.Check{
			Name: "mongo",
			Func: func(ctx context.Context) error { return client.Ping(ctx, nil) },
		}),
	)
	if err != nil {
		logger.ErrorContext(ctx, "server", log.Err(err))
		return
	}
	if err := srv.Run(ctx); err != nil {
		logger.ErrorContext(ctx, "server", log.Err(err))
	}
}

// Build the options, adjust what Config does not cover, then connect.
// Connect still installs the OTel bridge and pings the primary.
func ExampleClientOptions() {
	ctx := context.Background()
	co, err := mongox.ClientOptions(mongox.Config{URI: os.Getenv("MONGO_URI"), AppName: "reports"})
	if err != nil {
		return
	}
	co.SetReadPreference(readpref.SecondaryPreferred())
	co.SetCompressors([]string{"zstd", "snappy"})

	client, err := mongox.Connect(ctx, co)
	if err != nil {
		return
	}
	defer func() { _ = client.Disconnect(context.WithoutCancel(ctx)) }()
}

// A monitor set before Connect replaces the matching half of the OTel
// bridge: here the command spans and duration metric stay, but
// db.client.connection.count is not recorded.
func ExampleConnect_poolMonitor() {
	ctx := context.Background()
	logger := log.Default()
	co, err := mongox.ClientOptions(mongox.Config{URI: os.Getenv("MONGO_URI")})
	if err != nil {
		return
	}
	co.SetPoolMonitor(&event.PoolMonitor{Event: func(e *event.PoolEvent) {
		if e.Type == event.ConnectionPoolCleared {
			logger.Warn("mongo pool cleared", "address", e.Address)
		}
	}})

	client, err := mongox.Connect(ctx, co, mongox.WithLogger(logger))
	if err != nil {
		return
	}
	defer func() { _ = client.Disconnect(context.WithoutCancel(ctx)) }()
}

func ExampleMapError() {
	err := mongox.MapError(fmt.Errorf("get order: %w", mongo.ErrNoDocuments))
	fmt.Println(errors.Is(err, errs.ErrNotFound), errs.Code(err))

	dup := mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000, Message: "E11000 duplicate key"}}}
	err = mongox.MapError(dup)
	fmt.Println(errors.Is(err, errs.ErrAlreadyExists), errs.Code(err))
	// Output:
	// true NotFound
	// true AlreadyExists
}
